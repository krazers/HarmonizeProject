#!/usr/bin/python3

########################################
########## Harmonize Project ###########
##########        by         ###########
########## MCP Capital, LLC  ###########
########################################
# Github.com/MCPCapital/harmonizeproject
# Script Last Updated - Release 1.2.0
# Modifications by Chris Azer, added support
# for integration with AWS IoT Greengrass v2
########################################
### -v to enable verbose messages     ##
### -g # to pre-select a group number ##
### -b # to pre-select a bridge by id ##
### -s # single light source optimized #
########################################

import sys
import IPCUtils as ipc_utils
from http_parser.parser import HttpParser
import argparse
import requests 
import time
import json
from pathlib import Path
from socket import socket, AF_INET, SOCK_DGRAM, IPPROTO_UDP, timeout
import subprocess
import threading
import fileinput
import numpy as np
import cv2
import math
import colorconverter
from datetime import datetime

import config_utils
#ipc_utils.IPCUtils().publish_results_to_pubsub_ipc(PAYLOAD)

convert = colorconverter.Converter()
parser = argparse.ArgumentParser()
parser.add_argument("-v","--verbose", dest="verbose", action="store_true")
parser.add_argument("-g","--groupid", dest="groupid")
parser.add_argument("-b","--bridgeid", dest="bridgeid")
parser.add_argument("-s","--single_light", dest="single_light", action="store_true")
commandlineargs = parser.parse_args()

is_single_light = False
stopped = False
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)
    
def verbose(*args, **kwargs):
    if commandlineargs.verbose==True:
        print(*args, **kwargs)

######### Initialization Complete - Now lets try and connect to the bridge ##########

def findhue():  #Auto-find bridges on network & get list
    r = requests.get("https://discovery.meethue.com/")
    bridgelist = json.loads(r.text)
    i = 0
    for b in bridgelist:
        i += 1
    
    if commandlineargs.bridgeid is not None:
        found = False
        for idx, b in enumerate(bridgelist):
            if b["id"] == commandlineargs.bridgeid:
                bridge = idx
                found = True
                break
        if not found:
            sys.exit("bridge {} was not found".format(commandlineargs.bridgeid))
    elif len(bridgelist)>1:
        print("Multiple bridges found. Select one of the bridges below (", list(bridgelist),")")
        bridge = int(input())   
    else: 
        bridge = 0 #Default to the only bridge if only one is found
     
    hueip = bridgelist[bridge]['internalipaddress'] #Logic currently assumes 1 bridge on the network
    print("I will use the bridge at ", hueip)
    
    msg = \
        'M-SEARCH * HTTP/1.1\r\n' \
        'HOST:' + hueip +':1900\r\n' \
        'ST:upnp:rootdevice\r\n' \
        'MX:2\r\n' \
        'MAN:"ssdp:discover"\r\n' \
        '\r\n'
    s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP)
    s.settimeout(12)
    s.sendto(msg.encode('utf-8'), (hueip, 1900) )
    try:
        while True:
            data, addr = s.recvfrom(65507)
            p = HttpParser()
            recved = len(data)
            nparsed = p.execute(data, recved)
            assert nparsed == recved
            if p.is_headers_complete():
                headers = p.get_headers()
                if 'hue-bridgeid' in headers:
                    return addr,headers
            if p.is_message_complete():
                break
    except timeout:
        verbose('Timed out, better luck next time')
        pass
    return None

def register():
    print("Device not registered on bridge")
    payload = {"devicetype":"harmonizehue","generateclientkey":True}
    print("You have 45 seconds to push the button! I will check if you did every 5 seconds")
    attempts = 1
    while attempts < 10:
        r = requests.post(("http://%s/api" % (hueip)), json.dumps(payload))
        bridgeresponse = json.loads(r.text)
        if  'error' in bridgeresponse[0]:
            print(attempts,"Warning: {0}".format(bridgeresponse[0]['error']['description']))
        elif('success') in bridgeresponse[0]:
            clientdata = bridgeresponse[0]["success"]
            did_get_username = True
            f = open("client.json", "w")
            f.write(json.dumps(clientdata))
            f.close()
            print("Success! I generated a username and client key to access the bridge's Entertainment API!")
            break
        else:
            print("No response")
        attempts += 1
        time.sleep(5)
    else:
        print("You didn't push the button...  Exiting...")
        exit()

def enablestreaming():
    ##### Setting up streaming service and calling the DTLS handshake command ######
    print("Enabling streaming on your Entertainment area") #Allows us to send UPD to port 2100
    r = requests.put(url = baseurl+"/{}/groups/{}".format(clientdata['username'],groupid),json={"stream":{"active":True}})
    jsondata = r.json()

def setup():
    global light_locations, clientdata, hueip, groupid, bufferlock
    #verbose("Finding bridge...")
    (hueip,port),headers = findhue() or ((None,None),None)
    if hueip is None:
        sys.exit("Hue bridge not found. Mission failed, better luck next time")
    verbose("I found the Bridge on", hueip)
    verbose("Checking if Harmonize is registered on the bridge... (Looking for client.json)") #Check if the username and client key have already been saved

    if Path("./client.json").is_file():
        f = open("client.json", "r")
        jsonstr = f.read()
        clientdata = json.loads(jsonstr)
        f.close()
        verbose("Client Data Found)")
        global baseurl
        baseurl = "http://{}/api".format(hueip)
        setupurl = baseurl + "/" + clientdata['username']
        r = requests.get(url = setupurl)
        setupresponse = dict()
        setupresponse = json.loads(r.text)
        if  setupresponse.get('error'):
            verbose("Client data no longer valid")
            register()
        else:
            verbose("Client data valid", clientdata)
    else:
        register()

    verbose("Requesting bridge information...") #Make sure bridge supports streaming API
    r = requests.get(url = baseurl+"/config")
    jsondata = r.json()
    if jsondata["apiversion"]<"1.22":
        sys.exit("Bridge is way too old! Upgrade it to 1.22+ in the Hue app.")
    verbose("Api version is good to go. You've got version {}...".format(jsondata["apiversion"]))

    ######### We're connected! - Now lets find entertainment areas in the list of groups ##########
    r = requests.get(url = baseurl+"/{}/groups".format(clientdata['username']))
    jsondata = r.json()
    groups = dict()
    groupid = commandlineargs.groupid

    if groupid is not None:
        verbose("Checking for entertainment group {}".format(groupid))
    else:
        verbose("Checking for entertainment groups (not none)")

    for k in jsondata:  #These 3 sections isolate Entertainment areas from normal groups (like rooms)
        if jsondata[k]["type"]=="Entertainment":
            if groupid is None or k==groupid:
                groups[k] = jsondata[k]

    if len(groups)==0: #No groups or null = exit
        if groupid is not None:
            sys.exit("Entertainment group not found, set one up in the Hue App according to the instructions on github.")
        else:
            sys.exit("Entertainment group not found, set one up in the Hue App according to the instructions on github.")

    if len(groups)>1:
        eprint("Multiple entertainment groups found (", groups,") specify which with --groupid")
        for g in groups:
            eprint("{} = {}".format(g,groups[g]["name"]))
        groupid = input()
        print("You selected groupid ", groupid)
        #sys.exit()

    if groupid is None:
        groupid=next(iter(groups))
    verbose("Using groupid={}".format(groupid))

    #### Lets get the lights & their locations in our selected group and enable streaming ######
    for l in jsondata:
        r = requests.get(url = baseurl+"/{}/groups/{}".format(clientdata['username'],groupid))
        jsondata = r.json()
        light_locations = dict()
        light_locations = jsondata['locations']
    verbose("These are the lights and locations found: \n", light_locations)

    enablestreaming()

    ######### Prepare the messages' vessel for the RGB values we will insert
    bufferlock = threading.Lock()
    stopped = False
    def stdin_to_buffer():
        for line in fileinput.input():
            print(line)
            if stopped:
                break

######This is used to execute the command near the bottom of this document to create the DTLS handshake with the bridge on port 2100
def execute(cmd):
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    verbose("Executing send commands... Cross your fingers")
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()
    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)

######################################################
### Scaling light locations and averaging colors #####
######################################################

def averageimage():
########## Scales up locations to identify the nearest pixel based on lights' locations #######
    time.sleep(1.2) #wait for video size to be defined
    for x, coords in light_locations.items():
        coords[0] = ((coords[0])+1) * w//2 #Translates x value and resizes to video aspect ratio
        coords[2] = (-1*(coords[2])+1) * h//2 #Flips y, translates, and resize to vid aspect ratio
        
    #for x, y in light_locations.items(): #Defines locations by light
        #time.sleep(.01)
    scaled_locations = list(light_locations.items()) #Makes it a list of locations by light
    verbose("Lights and locations (in order) on TV array after math are: ", scaled_locations)
  
#### This section assigns light locations to variable light1,2,3...etc. in JSON order
    avgsize = w/2 + h/2
    verbose('avgsize is', avgsize)
    breadth = .30 #approx percent of the screen outside the location to capture
    dist = int(breadth*avgsize) #Proportion of the pixels we want to average around in relation to the video size
    verbose('Distance from relative location is: ', dist)

    global cords #array of coordinates
    global bounds #array of bounds for each coord, each item is formatted as [top, bottom, left, right]
    
    #initialize the arrays
    cords = {}
    bounds = {}
    for num, cds in scaled_locations:
        #cords.append(cds)
        cords[num] = cds
        bds = [cds[2] - dist, cds[2] + dist, cds[0] - dist, cds[0] + dist]
        bds = list(map(int, bds))
        bds = list(map(lambda x: 0 if x < 0 else x, bds))
        #bounds.append(bds)
        bounds[num] = bds
   
    global rgb,rgb_bytes #array of rgb values, one for each light
    rgb = {}
    rgb_bytes = {}
    area = {}

# Constantly sets RGB values by location via taking average of nearby pixels
    while not stopped:
        for x, bds in bounds.items():
            #area[x] = rgbframe[bds[2]:bds[3], bds[0]:bds[1], :]
            area[x] = rgbframe[bds[0]:bds[1], bds[2]:bds[3], :]
            rgb[x] = cv2.mean(area[x])
        for x, c in rgb.items():
            xy = convert.rgb_to_xy(c[0],c[1],c[2])
            rgb_with_brightness = convert.xy_to_rgb(xy[0],xy[1],bri=1)
            #rgb_with_brightness = bytes.fromhex(convert.xy_to_hex(xy[0],xy[1],bri=.5))
            rgb_bytes[x] = bytearray([int(rgb_with_brightness[0]/2), int(rgb_with_brightness[0]/2), int(rgb_with_brightness[1]/2), int(rgb_with_brightness[1]/2), int(rgb_with_brightness[2]/2), int(rgb_with_brightness[2]/2),] )

######################################################
############ Video Capture Setup #####################
######################################################

######### Now that weve defined our RGB values as bytes, we define how we pull values from the video analyzer output
def cv2input_to_buffer(): ######### Section opens the device, sets buffer, pulls W/H
    global w,h,rgbframe, channels
    cap = cv2.VideoCapture(1, cv2.CAP_V4L2) #variable cap is our raw video input. This is specific to a jetson that also has a camera installed on video0
    if cap.isOpened(): # Try to get the first frame
        verbose('Capture Device Opened')
    else: #Makes sure we can access the device
        sys.exit('Unable to open Capture Device') #quit
    w  = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))  # gets video width
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT)) # gets video height
    verbose('Video Shape is: ', w, h) #prints video shape

########## This section loops & pulls re-colored frames and alwyas get the newest frame 
    cap.set(cv2.CAP_PROP_BUFFERSIZE,1) # No frame buffer to avoid lagging, always grab newest frame
    ct = 0 ######ct code grabs every X frame as indicated below
    while not stopped:
        ct += 1
        ret = cap.grab() #constantly grabs frames
        if ct % 1 == 0: # Skip frames (1=don't skip,2=skip half,3=skip 2/3rds)
            ret, bgrframe = cap.retrieve() #processes most recent frame
            if is_single_light:
                channels = cv2.mean(bgrframe)
            else:
                rgbframe = cv2.cvtColor(bgrframe, cv2.COLOR_BGR2RGB) #corrects BGR to RGB
                #verbose('BGrframe is :',bgrframe)
            if not ret: break

def set_configuration(config):
    r"""
    Sets a new config object with the combination of updated and default configuration as applicable.
    Calls inference code with the new config and indicates that the configuration changed.
    """
    new_config = {}

    if "SubscribeToTopic" in config:
        config_utils.TOPIC = config["SubscribeToTopic"]
    else:
        config_utils.TOPIC = ""
        config_utils.logger.warning("Topic to publish inference results is empty.")


######################################################
############## Sending the messages ##################
######################################################

######### This is where we define our message format and insert our light#s, RGB values, and X,Y,Brightness ##########
def buffer_to_light(): #Potentially thread this into 2 processes?
    cmd = ["openssl","s_client","-dtls1_2","-cipher","PSK-AES128-GCM-SHA256","-psk_identity",clientdata['username'],"-psk",clientdata['clientkey'], "-connect", hueip+":2100"]
    proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stdin=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)            
    lastchangetime=datetime.now()
    lastmessage=bytes('HueStream','utf-8')
    disabledstreaming = False
    time.sleep(1.5) #Hold on so DTLS connection can be made & message format can get defined
    while not stopped:
        try:
            bufferlock.acquire()
            
            message = bytes('HueStream','utf-8') + b'\1\0\0\0\0\0\0'
            if is_single_light:
                single_light_bytes = bytearray([int(channels[2]/2), int(channels[2]/2), int(channels[1]/2), int(channels[1]/2), int(channels[0]/2), int(channels[0]/2),] ) # channels corrected here from BGR to RGB
                message += b'\0\0' + bytes(chr(int(1)), 'utf-8') + single_light_bytes
            else:
                for i in rgb_bytes:
                    message += b'\0\0' + bytes(chr(int(i)), 'utf-8') + rgb_bytes[i]

            lastmessage = message
            lastchangetime = datetime.now()

            bufferlock.release()
            proc.stdin.write(message.decode('utf-8','ignore'))
            time.sleep(.01) #0.01 to 0.02 (slightly under 100 or 50 messages per sec // or (.015 = ~66.6))
            proc.stdin.flush()

            #if(str(lastmessage) != str(message) or True):
            #    if(disabledstreaming):
            #        print('Enabling Streaming...') 
            #        disabledstreaming = False
            #        stopped=True #Force a rstart     
                
                #verbose('Wrote message and flushed. Briefly waiting') #This will verbose after every send, spamming the console.
            #else:
            #    if(disabledstreaming == False):
            #        if((datetime.now()-lastchangetime).total_seconds()>15):
            #            print('Disabling Streaming...') 
            #            disabledstreaming = True
            #            disablestreaming()
            #            bufferlock.release()
            #            time.sleep(.01)
            #        else: 
            #            bufferlock.release()
            #            proc.stdin.write(message.decode('utf-8','ignore'))
            #            time.sleep(.01) #0.01 to 0.02 (slightly under 100 or 50 messages per sec // or (.015 = ~66.6))
            #            proc.stdin.flush()
            #    else:
            #        bufferlock.release()
            #        time.sleep(.01)


        except Exception as e:
            pass

def disablestreaming():
    print("Disabling streaming on Entertainment area")
    r = requests.put(url = baseurl+"/{}/groups/{}".format(clientdata['username'],groupid),json={"stream":{"active":False}}) 
    jsondata = r.json()
    verbose(jsondata)

######################################################
############### Initialization Area ##################
######################################################

def initialize():
    stopped = False
    setup()
    ######### Section executes video input and establishes the connection stream to bridge ##########
    try:
        try:
            threads = list()
            verbose("Starting cv2input...")
            try:
                subprocess.check_output("ls -ltrh /dev/video1",shell=True)
            except subprocess.CalledProcessError:                                                                                                  
                print("--- ERROR: Video capture card not detected on /dev/video1 ---")
            else:
                print("--- INFO: Detected video capture card on /dev/video1 ---")
                t = threading.Thread(target=cv2input_to_buffer)
                t.start()
                threads.append(t)
                time.sleep(.75)
                if (commandlineargs.single_light is True) and (len(light_locations)==1):
                    is_single_light = True
                    print("Enabled optimization for single light source") # averager thread is not utilized
                else:
                    is_single_light = False
                    verbose("Starting image averager...")
                    t = threading.Thread(target=averageimage)
                    t.start()
                    threads.append(t)
                time.sleep(.25) #Initialize and find bridge IP before creating connection
                verbose("Opening SSL stream to lights...")
                t = threading.Thread(target=buffer_to_light)
                t.start()
                threads.append(t)

                set_configuration(ipc_utils.IPCUtils().get_configuration())        
                ipc_utils.IPCUtils().subscribe_to_cloud(config_utils.TOPIC)

                while not stopped:
                    time.sleep(10)
                
                for t in threads:
                    t.join()
        except Exception as e:
            print(e)

    finally: #Turn off streaming to allow normal function immedietly
        disablestreaming()

while True:
    print("Initializing...")
    initialize()
