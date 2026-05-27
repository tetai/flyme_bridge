1、因扫码登录功能依赖原子能力和智慧生活未调通，/api/ha/bindRet接口直接mock本地MQTT数据，见smart_lift_client.py bind_ret方法，注意修改haClientId，clientId一样会导致互相踢下线。
return ApiResponse(
            success=True,
            data={
                "mqttHost": "172.16.215.179",
                "mqttPort": 1883,
                "mqttToken": "",
                "haClientId": "123456",
            },
            message="Mocked bind result",
        )
2、临时调试，增加订阅topic，flyme_mqtt_connector.py _on_connect