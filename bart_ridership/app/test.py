from app.loader import BartRidershipLoader


def hello(event, context):
    loader = BartRidershipLoader(2019, 2019)
    loader.run()
