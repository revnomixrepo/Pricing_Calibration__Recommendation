from flask_restful import Resource
import sys
sys.path.insert(0,r'C:\Programs/')

import RevSeed_HNF.auto_mail_download as amd
import RevSeed_HNF.HnF_Templetes as hnf


class HNFController(Resource):

    @classmethod
    def get(self):
       return {'message': 'thanks'}

    @classmethod
    def post(self):
        amd.emailAttachment()
        hnf.run_hnf()
        ## Calibration = CalibrationService(pid)
        return {'message': 'HnF run successfully .....'}
