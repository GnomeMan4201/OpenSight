#!/bin/bash
# scripts/stop.sh — stop all OpenSight services
pkill -f "uvicorn apps.api.main" 2>/dev/null && echo "✓ API stopped" || echo "  API was not running"
pkill -f "semantic_service.py"   2>/dev/null && echo "✓ Semantic stopped" || echo "  Semantic was not running"
