"""API route handlers"""

import re
from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import HTTPBearer
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from app.schemas import (
    KeywordInput,
    GenerationRequest,
    FinalOutput,
    StrategyBrief
)
from app.orchestration.pipeline import RankForgePipeline
from app.utils.logger import setup_logger
from app.utils.exceptions import RankForgeException
from app.utils.database import get_db
from jose import jwt, JWTError
from app.config import settings

logger = setup_logger(__name__)
router = APIRouter(prefix="/api/v1", tags=["RankForge"])

_security = HTTPBearer(auto_error=False)
JWT_ALGORITHM = "HS256"


def _get_user_id(credentials=Depends(_security)) -> Optional[str]:
    """Extract user_id from JWT bearer token if present, else None."""
    if credentials is None:
        return None
    try:
        payload = jwt.decode(
            credentials.credentials, settings.jwt_secret, algorithms=[JWT_ALGORITHM]
        )
        return payload.get("sub")
    except JWTError:
        return None

def get_current_user_id(user_id: Optional[str] = Depends(_get_user_id)) -> str:
    """Strictly require authentication, returning the user_id or throwing 401."""
    if not user_id:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication required to perform this action")
    return user_id

def _check_api_key_error(error: Exception) -> bool:
    error_str = str(error).lower()
    return any(k in error_str for k in [
        "invalid api key", "invalid_api_key", "authentication",
        "401", "unauthorized", "api key not found", "api_key_invalid"
    ])


# ── Generate ────────────────────────────────────────────────────────────────

@router.post("/generate", response_model=FinalOutput, status_code=status.HTTP_200_OK)
async def generate_blog(
    request: GenerationRequest,
    user_id: str = Depends(get_current_user_id),
) -> FinalOutput:
    """Run the full 3-phase pipeline and persist to MongoDB."""
    logger.info(f"Generate request: {request.keyword_input.primary_keyword} | user={user_id}")

    try:
        pipeline = RankForgePipeline()
        result = await pipeline.execute(request)
        logger.info("Blog generation completed successfully")

        # Persist to MongoDB when user is authenticated
        if user_id:
            db = get_db()
            if db is not None:
                try:
                    title_match = re.search(r'^#\s+(.+)$', result.blog_content, re.MULTILINE)
                    title = title_match.group(1) if title_match else "Untitled Blog"
                    clean = re.sub(r'[#*`\[\]()]', '', result.blog_content)
                    word_count = len(clean.split())
                    await db.generations.insert_one({
                        "user_id": user_id,
                        "title": title,
                        "keyword": result.strategy_brief["primary_keyword"],
                        "seo_score": round(result.metadata.overall_score),
                        "naturalness": round(result.metadata.naturalness_analysis.naturalness_score),
                        "word_count": word_count,
                        "blog_content": result.blog_content,
                        "metadata": result.metadata.model_dump(),
                        "strategy_brief": result.strategy_brief,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                        "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                    })
                    logger.info(f"Generation saved to MongoDB for user {user_id}")
                except Exception as db_err:
                    logger.warning(f"Failed to save generation to DB (non-fatal): {db_err}")

        return result

    except RankForgeException as e:
        logger.error(f"RankForge error: {e.message}")
        if _check_api_key_error(e):
            raise HTTPException(status_code=401, detail={"error": "Invalid Groq API Key", "message": str(e.message)})
        raise HTTPException(status_code=500, detail={"error": e.message, "details": e.details})
    except Exception as e:
        logger.error(f"Unexpected error in generate: {str(e)}")
        if _check_api_key_error(e):
            raise HTTPException(status_code=401, detail={"error": "Invalid Groq API Key", "message": str(e)})
        raise HTTPException(status_code=500, detail={"error": "Internal server error", "message": str(e)})


# ── History ─────────────────────────────────────────────────────────────────

@router.get("/history", status_code=status.HTTP_200_OK)
async def get_history(user_id: Optional[str] = Depends(_get_user_id)) -> List[Dict]:
    """Fetch generation history for the authenticated user."""
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    cursor = db.generations.find(
        {"user_id": user_id},
        {"blog_content": 0}  # exclude heavy field from list view
    ).sort("timestamp", -1).limit(50)

    items = []
    async for doc in cursor:
        doc["id"] = str(doc.pop("_id"))
        items.append(doc)

    return items


@router.get("/history/{item_id}", status_code=status.HTTP_200_OK)
async def get_history_item(item_id: str, user_id: Optional[str] = Depends(_get_user_id)) -> Dict:
    """Fetch a single generation including full blog content."""
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    from bson import ObjectId
    try:
        doc = await db.generations.find_one({"_id": ObjectId(item_id), "user_id": user_id})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID")

    if not doc:
        raise HTTPException(status_code=404, detail="Not found")

    doc["id"] = str(doc.pop("_id"))
    return doc


@router.delete("/history/{item_id}", status_code=status.HTTP_200_OK)
async def delete_history_item(item_id: str, user_id: Optional[str] = Depends(_get_user_id)) -> Dict:
    """Delete a generation from history."""
    if not user_id:
        raise HTTPException(status_code=401, detail="Authentication required")

    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="Database not available")

    from bson import ObjectId
    try:
        result = await db.generations.delete_one({"_id": ObjectId(item_id), "user_id": user_id})
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid ID")

    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Not found")

    return {"deleted": True}


# ── Keyword Analysis ─────────────────────────────────────────────────────────

@router.post("/analyze-keywords", response_model=StrategyBrief, status_code=status.HTTP_200_OK)
async def analyze_keywords(
    keyword_input: KeywordInput,
    user_id: str = Depends(get_current_user_id)
) -> StrategyBrief:
    logger.info(f"Keyword analysis request: {keyword_input.primary_keyword} | user={user_id}")
    try:
        pipeline = RankForgePipeline()
        result = await pipeline.analyze_keywords_only(keyword_input)
        return result
    except RankForgeException as e:
        if _check_api_key_error(e):
            raise HTTPException(status_code=401, detail={"error": "Invalid Groq API Key"})
        raise HTTPException(status_code=500, detail={"error": e.message})
    except Exception as e:
        if _check_api_key_error(e):
            raise HTTPException(status_code=401, detail={"error": "Invalid Groq API Key"})
        raise HTTPException(status_code=500, detail={"error": str(e)})


# ── Health ───────────────────────────────────────────────────────────────────

@router.get("/health", status_code=status.HTTP_200_OK)
async def health_check() -> Dict[str, Any]:
    return {"status": "healthy", "service": "RankForge", "version": "1.0.0"}
