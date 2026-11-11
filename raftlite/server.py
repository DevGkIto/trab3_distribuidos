"""Servicos gRPC: finos adaptadores sobre o Node."""
from .pb import pbg


class ClusterServicer(pbg.ClusterServiceServicer):
    def __init__(self, node):
        self.node = node

    def Fetch(self, request, context):
        return self.node.handle_fetch(request)

    def RequestVote(self, request, context):
        return self.node.handle_request_vote(request)

    def GetStatus(self, request, context):
        return self.node.handle_status()


class ClientServicer(pbg.ClientServiceServicer):
    def __init__(self, node):
        self.node = node

    def Write(self, request, context):
        return self.node.handle_write(request)

    def Read(self, request, context):
        return self.node.handle_read(request)