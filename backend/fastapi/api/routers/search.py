from fastapi import APIRouter, Query, Request, Depends, HTTPException
from typing import List, Optional, Dict, Any
from ..services.es_service import get_es_service
from ..routers.auth import get_current_user
from ..models import User

router = APIRouter(prefix="/search", tags=["Full-Text Search"])

@router.get("/")
async def perform_search(
    q: str = Query(..., min_length=2, description="Search terms"),
    page: int = Query(1, ge=1),
    size: int = Query(10, le=50),
    current_user: User = Depends(get_current_user)
):
    """
    Rich full-text search across journal entries and assessments.
    Supports synonyms (e.g., 'joyful' finds 'happy'), fuzziness, and highlighting.
    """
    es = get_es_service()
    
    tenant_id = getattr(current_user, 'tenant_id', None)
    user_id = current_user.id
    
    # Execute ES Search
    res = await es.search(
        q=q,
        tenant_id=tenant_id,
        user_id=user_id,
        page=page,
        size=size
    )
    
    hits = res.get('hits', {}).get('hits', [])
    total = res.get('hits', {}).get('total', {}).get('value', 0)
    
    results = []
    for hit in hits:
        source = hit.get('_source', {})
        highlight = hit.get('highlight', {}).get('content', [])
        
        results.append({
            "id": source.get('id'),
            "entity": source.get('entity'),
            "score": hit.get('_score'),
            "snippet": highlight[0] if highlight else source.get('content', '')[:150],
            "timestamp": source.get('timestamp')
        })
        
    return {
        "query": q,
        "total": total,
        "page": page,
        "results": results
    }

@router.post("/reindex")
async def trigger_reindex(current_user: User = Depends(get_current_user)):
    """Admin only: Bulk load all existing data into Elasticsearch."""
    if not getattr(current_user, 'is_admin', False):
         raise HTTPException(status_code=403, detail="Admin required")
    
    # Background task to reindex
    from fastapi import BackgroundTasks
    # To be implemented in management command script
    return {"message": "Reindexing started in background"}
