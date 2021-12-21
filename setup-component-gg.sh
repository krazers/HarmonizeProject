###############################################################################
## Global Parameters                                                         ##
###############################################################################
export region=us-west-2
export acct_num=$(aws sts get-caller-identity --query "Account" --output text)
export component_version=1.0.5
corename="HueSyncCore"
# CF parameters
export demo_name="philipshue"
#export statustopic="flightdata/status"
export artifact_bucket_name=$demo_name-component-artifacts-$acct_num-$region

###############################################################################
## Prereqs                                                                   ##
###############################################################################
echo "###############################################################################"
echo "## Setup prerequisites..."
sudo yum install jq -y

###############################################################################
## Create component that sets up S3 streams in  Stream Manager               ##
## This includes a low and high priority stream. In addition, it monitors    ##
## data transfer state changes from another stream and sends the results to  ##
## IoT Core on topic 'flightdata/status'                                     ##
###############################################################################

echo "###############################################################################"
echo "## Create Philip Hue Sync Component for Greengrass..."

# export variables
export component_name=com.azer.philipshue.sync

# Create artifact for component
mkdir -p ~/GreengrassCore/artifacts/$component_name/$component_version
cp * ~/GreengrassCore/artifacts/$component_name/$component_version -r
(cd ~/GreengrassCore/artifacts/$component_name/$component_version/; zip -m -r $component_name.zip * )

aws s3 mb s3://$artifact_bucket_name

# and copy the artifacts to S3
aws s3 sync ~/GreengrassCore/ s3://$artifact_bucket_name/

# create recipe for component
mkdir -p ~/GreengrassCore/recipes/
cat ~/GreengrassCore/recipes/$component_name-$component_version.json

uri=s3://$artifact_bucket_name/artifacts/$component_name/$component_version/$component_name.zip
script="python3 -m pip install awsiotsdk; python3 -u {artifacts:decompressedPath}/$component_name/harmonize.py"
topic="\$aws/things/$corename/shadow/name/tv"
topic2="$corename/get/accepted"
json=$(jq --null-input \
  --arg component_name "$component_name" \
  --arg component_version "$component_version" \
  --arg script "$script" \
  --arg uri "$uri" \
  --arg topic "$topic" \
  --arg topic2 "$topic2" \
'{ "RecipeFormatVersion": "2020-01-25", 
"ComponentName": $component_name, 
"ComponentVersion": $component_version, 
"ComponentDescription": "A component that monitors /dev/video1 source to identify colors and change philips hue light colors respectively", 
"ComponentPublisher": "Azer",
"ComponentConfiguration": {
    "DefaultConfiguration": {
      "accessControl": {
          "aws.greengrass.ShadowManager": {
              "<component_name>:shadow:1": {   
                    "policyDescription": "Allows access to shadows",
                    "operations": [
                    "aws.greengrass#GetThingShadow",
                    "aws.greengrass#UpdateThingShadow",
                    "aws.greengrass#DeleteThingShadow"
                    ],
                    "resources": [
                       $topic
                    ]
                }  
            },     
            "aws.greengrass.ipc.pubsub": {
                "<component_name>:pubsub:1": {
                    "policyDescription": "Allows access to shadow pubsub topics",
                    "operations": [
                    "aws.greengrass#SubscribeToTopic"
                    ],
                    "resources": [
                        $topic2
                    ]
                }
            }
        }
    }
},
"Manifests": [ { "Platform": { "os": "linux" }, 
"Lifecycle": { "RequiresPrivilege": false, "Run": $script }, 
"Artifacts": [ { "URI": $uri, 
"Unarchive": "ZIP", "Permission": { "Read": "ALL", "Execute": "NONE" } } ] } ] }')

# Create recipe file and component in Greengrass
echo ${json//<component_name>/$component_name} > ~/GreengrassCore/recipes/$component_name-$component_version.json
aws greengrassv2 create-component-version --inline-recipe fileb://~/GreengrassCore/recipes/$component_name-$component_version.json

echo "###############################################################################"
