"""Researcher Agent - Creates SEO-optimized content outlines"""

from typing import List
import json
from app.agents.base import BaseAgent
from app.schemas.keyword import StrategyBrief
from app.schemas.generation import ContentOutline, OutlineSection
from app.config import settings
from app.utils.exceptions import AgentExecutionError


class ResearcherAgent(BaseAgent):
    """
    Agent responsible for creating comprehensive, SEO-optimized content outlines
    based on keyword strategy and SERP analysis
    """
    
    def __init__(self):
        super().__init__(
            model_name=settings.researcher_model,
            temperature=0.7
        )
    
    async def execute(self, strategy_brief: StrategyBrief, tone: str = "professional") -> ContentOutline:
        """
        Create a comprehensive content outline
        
        Args:
            strategy_brief: Strategic brief from keyword engine
            tone: Content tone
            
        Returns:
            Detailed content outline
            
        Raises:
            AgentExecutionError: If outline generation fails
        """
        self.logger.info("Researcher Agent: Starting outline generation")
        
        try:
            # Build system prompt
            system_prompt = self._build_system_prompt()
            
            # Build user prompt with strategy brief
            user_prompt = self._build_user_prompt(strategy_brief, tone)
            
            # Invoke LLM
            response = await self._invoke_llm(system_prompt, user_prompt)
            
            # Parse response into structured outline
            outline = await self._parse_outline(response, strategy_brief)
            
            self.logger.info(f"Outline generated with {len(outline.sections)} main sections")
            return outline
            
        except Exception as e:
            self.logger.error(f"Researcher Agent failed: {str(e)}")
            raise AgentExecutionError(
                f"Failed to generate outline: {str(e)}",
                details={"agent": "ResearcherAgent", "error": str(e)}
            )
    
    def _build_system_prompt(self) -> str:
        """Build system prompt for researcher agent"""
        return """You are an elite SEO content strategist and a profound intellectual thinker. Your role is to create deeply logical, comprehensive content outlines that transcend basic generic articles.

Your outlines must employ First-Principle Thinking:
1. Deconstruct the core concept: What is the fundamental mechanism or truth behind this topic?
2. Build an undeniable logical progression (H2, H3, H4) that takes the reader from basic understanding to advanced mastery, step-by-step.
3. Avoid generic filler sections. Every section must have a strong, specific thesis.
4. Anticipate and answer profound, unasked questions (the "Why" behind the "What").
5. Identify featured snippet opportunities naturally.
6. Target specific keywords in each section without sacrificing flow.

You create outlines that writers can follow to produce high-ranking, masterclass-level essays.

Output your outline in the following JSON structure:
{
  "title": "SEO-optimized title with primary keyword",
  "meta_description": "Compelling 150-160 char description",
  "introduction_brief": "What the intro should cover",
  "sections": [
    {
      "heading": "Section heading with keywords",
      "heading_level": 2,
      "target_keywords": ["keyword1", "keyword2"],
      "snippet_opportunity": true/false,
      "estimated_word_count": 300,
      "subsections": [...]
    }
  ],
  "conclusion_brief": "What the conclusion should cover",
  "target_word_count": 2000,
  "primary_cta": "Call to action",
  "internal_links": [{"anchor": "text", "target": "url"}]
}"""
    
    def _build_user_prompt(self, strategy_brief: StrategyBrief, tone: str) -> str:
        """Build user prompt with strategy brief details"""
        
        cluster = strategy_brief.keyword_cluster
        serp_gap = strategy_brief.serp_gap
        traffic = strategy_brief.traffic_projection
        
        prompt = f"""Create a comprehensive content outline for the following:

PRIMARY KEYWORD: {cluster.primary}
TARGET LOCATION: {strategy_brief.target_location}
SEARCH INTENT: {cluster.search_intent}
CONTENT ANGLE: {strategy_brief.content_angle}
TONE: {tone}

SECONDARY KEYWORDS (must be incorporated):
{', '.join(cluster.secondary[:10])}

LONG-TAIL KEYWORDS:
{', '.join(cluster.long_tail[:10])}

RELATED QUESTIONS (prioritize for snippet optimization):
{chr(10).join(f'- {q}' for q in cluster.related_questions[:8])}

SERP GAP ANALYSIS - Missing Topics to Cover:
{chr(10).join(f'- {topic}' for topic in serp_gap.missing_topics)}

UNDERSERVED QUESTIONS:
{chr(10).join(f'- {q}' for q in serp_gap.underserved_questions)}

CONTENT OPPORTUNITIES:
{chr(10).join(f'- {opp}' for opp in serp_gap.content_opportunities)}

STRUCTURAL REQUIREMENTS:
- Minimum H2 sections: {strategy_brief.structural_requirements['min_h2_sections']}
- Minimum H3 subsections: {strategy_brief.structural_requirements['min_h3_subsections']}
- Target word count: {strategy_brief.structural_requirements['target_word_count']}
- Minimum internal links: {strategy_brief.structural_requirements.get('min_internal_links', 5)}

TRAFFIC PROJECTION:
- Estimated monthly searches: {traffic.estimated_monthly_searches}
- Competition: {traffic.competition_level}
- Ranking probability: {traffic.ranking_probability}%

INTERNAL LINKING OPPORTUNITIES:
{chr(10).join(f'- {link}' for link in strategy_brief.internal_linking_opportunities)}

Create a detailed outline that:
1. Addresses all related questions as H2 or H3 sections
2. Covers all missing topics identified in SERP gap
3. Optimizes for featured snippets (mark sections with snippet_opportunity: true)
4. Distributes keywords naturally across sections
5. Includes specific subsections with clear focus
6. Provides estimated word counts for each section
7. Suggests internal linking points

Return ONLY the JSON structure, no additional text."""
        
        return prompt
    
    async def _parse_outline(self, response: str, strategy_brief: StrategyBrief) -> ContentOutline:
        """Parse LLM response into ContentOutline object"""
        
        try:
            # Extract JSON from response
            json_start = response.find('{')
            json_end = response.rfind('}') + 1
            
            if json_start == -1 or json_end == 0:
                raise ValueError("No JSON found in response")
            
            json_str = response[json_start:json_end]
            data = json.loads(json_str)
            
            # Parse sections recursively
            sections = [self._parse_section(s) for s in data.get('sections', [])]
            
            # Create ContentOutline
            outline = ContentOutline(
                title=data.get('title', f"Complete Guide to {strategy_brief.keyword_cluster.primary}"),
                meta_description=(data.get('meta_description', '') or '')[:160] or f"Discover everything about {strategy_brief.keyword_cluster.primary}. Expert insights, practical tips, and actionable strategies for {strategy_brief.target_location}.",
                introduction_brief=data.get('introduction_brief', '') or f"Introduce {strategy_brief.keyword_cluster.primary}, its importance, and what readers will learn",
                sections=sections,
                conclusion_brief=data.get('conclusion_brief', ''),
                target_word_count=data.get('target_word_count', strategy_brief.structural_requirements['target_word_count']),
                primary_cta=data.get('primary_cta'),
                internal_links=data.get('internal_links', [])
            )
            
            return outline
            
        except json.JSONDecodeError as e:
            self.logger.warning(f"JSON parsing failed, creating fallback outline: {str(e)}")
            return self._create_fallback_outline(strategy_brief)
        except Exception as e:
            self.logger.warning(f"Outline parsing failed, creating fallback: {str(e)}")
            return self._create_fallback_outline(strategy_brief)
    
    def _parse_section(self, section_data: dict) -> OutlineSection:
        """Recursively parse section data"""
        
        subsections = []
        if 'subsections' in section_data:
            subsections = [self._parse_section(s) for s in section_data['subsections']]
        
        return OutlineSection(
            heading=section_data.get('heading', ''),
            heading_level=section_data.get('heading_level', 2),
            target_keywords=section_data.get('target_keywords', []),
            snippet_opportunity=section_data.get('snippet_opportunity', False),
            estimated_word_count=section_data.get('estimated_word_count', 200),
            subsections=subsections
        )
    
    def _create_fallback_outline(self, strategy_brief: StrategyBrief) -> ContentOutline:
        """Create a fallback outline if parsing fails"""
        
        cluster = strategy_brief.keyword_cluster
        serp_gap = strategy_brief.serp_gap
        
        sections = []
        
        # Introduction section
        sections.append(OutlineSection(
            heading=f"What is {cluster.primary}?",
            heading_level=2,
            target_keywords=[cluster.primary],
            snippet_opportunity=True,
            estimated_word_count=300
        ))
        
        # Add sections from related questions
        for i, question in enumerate(cluster.related_questions[:5]):
            sections.append(OutlineSection(
                heading=question,
                heading_level=2,
                target_keywords=[cluster.primary] + cluster.secondary[i:i+2],
                snippet_opportunity=True,
                estimated_word_count=250
            ))
        
        # Add sections from missing topics
        for topic in serp_gap.missing_topics[:3]:
            sections.append(OutlineSection(
                heading=topic,
                heading_level=2,
                target_keywords=cluster.secondary[:2],
                snippet_opportunity=False,
                estimated_word_count=300
            ))
        
        return ContentOutline(
            title=f"Complete Guide to {cluster.primary} in {strategy_brief.target_location}",
            meta_description=f"Discover everything about {cluster.primary} in {strategy_brief.target_location}. Expert insights, practical tips, and actionable strategies.",
            introduction_brief=f"Introduce {cluster.primary}, its importance, and what readers will learn",
            sections=sections,
            conclusion_brief=f"Summarize key points about {cluster.primary} and provide next steps",
            target_word_count=serp_gap.recommended_word_count,
            primary_cta=f"Get started with {cluster.primary} today",
            internal_links=[]
        )
