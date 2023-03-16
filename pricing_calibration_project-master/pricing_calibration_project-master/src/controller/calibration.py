from flask_restful import Resource

from CalibrationService import CalibrationService


class CalibrationController(Resource):

    @classmethod
    def get(self, pid: int = -1):
        return {'message': 'thanks', 'pid': pid}

    @classmethod
    def post(self, pid: int = -1):
        Calibration = CalibrationService(pid)
        return {'message': 'Calibration run successfully .....', 'pid': pid,
                'status': {"Calibration": Calibration}}
