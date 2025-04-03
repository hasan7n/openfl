from uvicorn.protocols.http.httptools_impl import HttpToolsProtocol

old_on_url = HttpToolsProtocol.on_url


def new_on_url(self, url):
    old_on_url(self, url)
    self.scope["transport"] = self.transport


HttpToolsProtocol.on_url = new_on_url
