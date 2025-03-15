from src.entities.clients.client import Client

#from entities.services.event_handler import EntitiesEventHandler

from ollama._types import (
    GenerateResponse,
    ChatResponse,
    ProgressResponse,
    Message,
    Options,
    RequestError,
    ResponseError,
)

__all__ = [
    'GenerateResponse',
    'ChatResponse',
    'ProgressResponse',
    'Message',
    'Options',
    'RequestError',
    'ResponseError',
    'Client',
    #'EntitiesEventHandler'

]

_client = Client()