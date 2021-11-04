from huey import RedisHuey

huey = RedisHuey('test', blocking=True, url="redis://192.168.50.76",)
