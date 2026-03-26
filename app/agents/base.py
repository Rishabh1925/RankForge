"""Base agent class for all AI agents"""

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage
from app.config import settings
from app.utils.logger import setup_logger, log_agent_activity
from app.utils.exceptions import AgentExecutionError

logger = setup_logger(__name__)

# Global semaphore to restrict concurrent LLM calls
# Helps maintain low RAM footprint for 512MB environments like Render
_llm_semaphore = asyncio.Semaphore(3)

class BaseAgent(ABC):
    """Abstract base class for all agents in the system"""
    
    def __init__(self, model_name: Optional[str] = None, temperature: float = 0.7):
        """
        Initialize base agent
        
        Args:
            model_name: Groq model name
            temperature: Model temperature
        """
        self.model_name = model_name or settings.researcher_model
        self.temperature = temperature
        self.logger = logger
        self.llm = self._initialize_llm()
    
    def _initialize_llm(self) -> ChatGroq:
        """Initialize the language model"""
        return ChatGroq(
            model=self.model_name,
            temperature=self.temperature,
            max_tokens=settings.max_tokens,
            groq_api_key=settings.groq_api_key,
            request_timeout=settings.request_timeout,
            model_kwargs={
                "presence_penalty": 0.4,
                "frequency_penalty": 0.4
            }
        )
    
    @abstractmethod
    async def execute(self, *args, **kwargs) -> Any:
        """Execute the agent's primary task"""
        pass
    
    async def _invoke_llm(
        self, 
        system_prompt: str, 
        user_prompt: str,
        context: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Invoke the LLM with system and user prompts
        
        Args:
            system_prompt: System instructions
            user_prompt: User query
            context: Additional context data
            
        Returns:
            LLM response text
            
        Raises:
            AgentExecutionError: If LLM invocation fails
        """
        try:
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=user_prompt)
            ]
            
            self.logger.debug(f"{self.__class__.__name__}: Waiting for LLM semaphore")
            
            async with _llm_semaphore:
                log_agent_activity(
                    self.logger, 
                    self.__class__.__name__, 
                    "Invoking LLM",
                    {"model": self.model_name}
                )
                
                response = await self.llm.ainvoke(messages)
            
            log_agent_activity(
                self.logger,
                self.__class__.__name__,
                "LLM response received",
                {"length": len(response.content)}
            )
            
            return response.content
            
        except Exception as e:
            self.logger.error(f"LLM invocation failed: {str(e)}")
            raise AgentExecutionError(
                f"Failed to invoke LLM: {str(e)}",
                details={"agent": self.__class__.__name__, "error": str(e)}
            )
    
    def _format_context(self, context: Dict[str, Any]) -> str:
        """Format context dictionary into readable string"""
        formatted = []
        for key, value in context.items():
            if isinstance(value, (list, dict)):
                formatted.append(f"{key}: {str(value)[:200]}...")
            else:
                formatted.append(f"{key}: {value}")
        return "\n".join(formatted)
