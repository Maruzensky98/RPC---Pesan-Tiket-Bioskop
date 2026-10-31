#!/usr/bin/env bash
python -m grpc_tools.protoc -I proto --python_out=src --grpc_python_out=src proto/bioskop.proto
echo "Stub dibuat di src/ (bioskop_pb2.py, bioskop_pb2_grpc.py)"
