from flask_restful import Resource


class Home(Resource):
    @classmethod
    def get(self):
        return {
            "name": "Pricing Recommendation and Calibration api",
            "api-version":"v.0.0.1"
        }
