from rest_framework.decorators import api_view
from rest_framework.response import Response
from django.db import connection


@api_view(["GET"])
def healthcheck(request):
    try:
        connection.ensure_connection()
        db_status = "ok"
    except Exception:
        db_status = "error"
    return Response({"status": "ok", "db": db_status})
