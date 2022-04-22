# create subscription
grpcurl -d @ -plaintext 127.0.0.1:2048  linkall.vanus.controller.TriggerController.CreateSubscription <<EOF
{
  "filters": [
      {
        "exact": {
            "type":"test"
        }
      }
  ],
  "sink": "http://127.0.0.1:18080",
  "eventBus": ""
}
EOF
# delete subscription
grpcurl -d @ -plaintext 127.0.0.1:2048  linkall.vanus.controller.TriggerController.DeleteSubscription <<EOF
{
  "id": 123
}
EOF



