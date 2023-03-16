from flask_restful import Resource

from RecommendationService import RecommendationService
from RecommendationService import uploadRateCM


class RecommendationController(Resource):

    @classmethod
    def get(self, pid: int = -1):
        return {'message': 'thanks', 'pid': pid}

    @classmethod
    def post(self, pid: int = -1):
        Recommendation = RecommendationService(pid)
        # update rate
        rate_push = uploadRateCM(pid)
        return {'message': 'Recommendation run successfully .....', 'pid': pid,
                'status': {'Recommendation': Recommendation, 'rate_push': rate_push}}
