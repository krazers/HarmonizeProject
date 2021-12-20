# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

from json import dumps
from os import getenv

import awsiot.greengrasscoreipc.client as client
import config_utils
from awscrt.io import (
    ClientBootstrap,
    DefaultHostResolver,
    EventLoopGroup,
    SocketDomain,
    SocketOptions,
)
from awsiot.eventstreamrpc import Connection, LifecycleHandler, MessageAmendment
from awsiot.greengrasscoreipc.model import (
    QOS,
    ConfigurationUpdateEvents,
    GetConfigurationRequest,
    PublishToIoTCoreRequest,
    PublishToTopicRequest,
    PublishMessage,
    JsonMessage,
    GetThingShadowRequest,
    UpdateThingShadowRequest,
    SubscribeToTopicRequest,
    SubscriptionResponseMessage
)


class IPCUtils:
    def connect(self):
        elg = EventLoopGroup()
        resolver = DefaultHostResolver(elg)
        bootstrap = ClientBootstrap(elg, resolver)
        socket_options = SocketOptions()
        socket_options.domain = SocketDomain.Local
        amender = MessageAmendment.create_static_authtoken_amender(getenv("SVCUID"))
        hostname = getenv("AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT")
        connection = Connection(
            host_name=hostname,
            port=8033,
            bootstrap=bootstrap,
            socket_options=socket_options,
            connect_message_amender=amender,
        )
        self.lifecycle_handler = LifecycleHandler()
        connect_future = connection.connect(self.lifecycle_handler)
        connect_future.result(config_utils.TIMEOUT)
        return connection

    def publish_results_to_cloud(self, PAYLOAD):
        r"""
        Ipc client creates a request and activates the operation to publish messages to the IoT core
        with a qos type over a topic.

        :param PAYLOAD: An dictionary object with inference results.
        """
        try:
            request = PublishToIoTCoreRequest(
                topic_name=config_utils.TOPIC,
                qos=config_utils.QOS_TYPE,
                payload=dumps(PAYLOAD).encode(),
            )
            operation = ipc_client.new_publish_to_iot_core()
            operation.activate(request).result(config_utils.TIMEOUT)
            config_utils.logger.info("Publishing results to the IoT core...")
            operation.get_response().result(config_utils.TIMEOUT)
        except Exception as e:
            config_utils.logger.error("Exception occured during publish: {}".format(e))

    def publish_results_to_pubsub_ipc(self, PAYLOAD):
        r"""
        Ipc client creates a request and activates the operation to publish messages to the Greengrass
        IPC Pubsub

        :param PAYLOAD: An dictionary object with inference results.
        """
        try:
            request = PublishToTopicRequest()
            request.topic = config_utils.TOPIC
            publish_message = PublishMessage()
            publish_message.json_message = JsonMessage()
            publish_message.json_message.message = PAYLOAD
            request.publish_message = publish_message
            operation = ipc_client.new_publish_to_topic()
            config_utils.logger.info("Publishing results to the Greengrass IPC Pubsub...")
            operation.activate(request)
            future = operation.get_response()
            future.result(config_utils.TIMEOUT)
        except Exception as e:
            config_utils.logger.error("Exception occured during publish: {}".format(e))

    def subscribe_to_cloud(self, topic):
        request = SubscribeToTopicRequest()
        request.topic = topic
        handler = StreamHandler()
        operation = ipc_client.new_subscribe_to_topic(handler) 
        future = operation.activate(request)
        future.result(config_utils.TIMEOUT)

    
    def sample_get_thing_shadow_request(thingName, shadowName):
        try:                    
            # create the GetThingShadow request
            get_thing_shadow_request = GetThingShadowRequest()
            get_thing_shadow_request.thing_name = thingName
            get_thing_shadow_request.shadow_name = shadowName
            
            # retrieve the GetThingShadow response after sending the request to the IPC server
            op = ipc_client.new_get_thing_shadow()
            op.activate(get_thing_shadow_request)
            fut = op.get_response()
            
            result = fut.result(config_utils.TIMEOUT)
            return result.payload
            
        except Exception as e:
            config_utils.logger.error(
                "Exception occured during fetching of shadow: {}".format(e)
            )
    
    def sample_update_thing_shadow_request(thingName, shadowName, payload):
        try:
            # create the UpdateThingShadow request
            update_thing_shadow_request = UpdateThingShadowRequest()
            update_thing_shadow_request.thing_name = thingName
            update_thing_shadow_request.shadow_name = shadowName
            update_thing_shadow_request.payload = payload
                            
            # retrieve the UpdateThingShadow response after sending the request to the IPC server
            op = ipc_client.new_update_thing_shadow()
            op.activate(update_thing_shadow_request)
            fut = op.get_response()
            
            result = fut.result(config_utils.TIMEOUT)
            return result.payload
            
        except Exception as e:
            config_utils.logger.error(
                "Exception occured while updating shadow: {}".format(e)
            )


class StreamHandler(client.SubscribeToTopicStreamHandler):
    def __init__(self):
        super().__init__()

    def on_stream_event(self, event: SubscriptionResponseMessage) -> None:
        try:
            message_string = str(event.binary_message.message, "utf-8")
            print(message_string)
            # Handle message.
        except:
            pass
            #traceback.print_exc()

    def on_stream_error(self, error: Exception) -> bool:
        # Handle error.
        return True  # Return True to close stream, False to keep stream open.

    def on_stream_closed(self) -> None:
        # Handle close.
        pass


# Get the ipc client
try:
    ipc_client = client.GreengrassCoreIPCClient(IPCUtils().connect())
    config_utils.logger.info("Created IPC client...")
except Exception as e:
    config_utils.logger.error(
        "Exception occured during the creation of an IPC client: {}".format(e)
    )
    exit(1)
