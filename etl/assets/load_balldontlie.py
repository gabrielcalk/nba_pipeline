class LoadBalldontlie:
    def __init__(self):
       

    def load(self):
        response = requests.get(self.url, headers=self.headers)
        return response.json()