"""Writer Agent - Generates SEO-optimized blog content"""

from app.agents.base import BaseAgent
from app.schemas.generation import ContentOutline, BlogDraft
from app.schemas.keyword import StrategyBrief
from app.config import settings
from app.utils.exceptions import AgentExecutionError
import re


class WriterAgent(BaseAgent):
    """
    Agent responsible for writing complete, SEO-optimized blog content
    based on the outline from Researcher Agent
    """
    
    def __init__(self):
        super().__init__(
            model_name=settings.writer_model,
            temperature=0.7
        )
    
    async def execute(
        self, 
        outline: ContentOutline, 
        strategy_brief: StrategyBrief,
        tone: str = "professional",
        include_faq: bool = True,
        custom_instructions: str = None
    ) -> BlogDraft:
        """
        Generate complete blog content from outline
        
        Args:
            outline: Content outline from Researcher
            strategy_brief: Original strategy brief
            tone: Writing tone
            include_faq: Whether to include FAQ section
            
        Returns:
            Complete blog draft
            
        Raises:
            AgentExecutionError: If content generation fails
        """
        self.logger.info("Writer Agent: Starting content generation")
        
        try:
            # Build system prompt
            system_prompt = self._build_system_prompt()
            
            # Build user prompt
            user_prompt = self._build_user_prompt(outline, strategy_brief, tone, include_faq, custom_instructions)
            
            # Invoke LLM
            markdown_content = await self._invoke_llm(system_prompt, user_prompt)
            
            # Post-process and validate
            markdown_content = self._post_process_content(markdown_content)
            draft = self._create_blog_draft(markdown_content, outline, strategy_brief)
            
            self.logger.info(f"Content generated: {draft.word_count} words, {draft.sections_count} sections")
            return draft
            
        except Exception as e:
            self.logger.error(f"Writer Agent failed: {str(e)}")
            raise AgentExecutionError(
                f"Failed to generate content: {str(e)}",
                details={"agent": "WriterAgent", "error": str(e)}
            )
    
    def _build_system_prompt(self) -> str:
        """Build system prompt for writer agent"""
        return """You are an elite SEO content writer and an intellectual essayist specializing in creating profound, high-ranking, conversion-focused blog posts.

Your writing must:
1. Follow the provided outline exactly
2. Optimize for featured snippets (concise answers, lists, tables)
3. Use keywords naturally without over-optimization
4. Write in a clear, highly engaging, and profound narrative style
5. Ensure paragraphs interlock logically instead of feeling disjointed
6. Include actionable insights and practical examples
7. Vary sentence structure and length for naturalness
8. Use strong transition words and phrases to build a compelling thought process
9. Format content in clean Markdown

CRITICAL QUALITY RULES - YOU MUST OBEY:
- NEVER repeat the same phrases, idioms, or transitional sentences.
- Use "Chain of Thought" reasoning: Introduce a concept, deconstruct its mechanism, and synthesize its impact.
- Ensure strict logical progression. Do NOT regurgitate the same point across different headings.
- Ban filler words, fluff, and generic AI phrases (e.g., "In today's fast-paced world", "Conclusion", "In summary").
- Use a highly diverse, expert-level vocabulary. Every single sentence must provide unique, profound value.

FEATURED SNIPPET OPTIMIZATION:
- Answer questions directly in 40-60 words
- Use numbered lists for step-by-step content
- Use bullet points for feature lists
- Use tables for comparisons
- Place answers immediately after question headings

NATURALNESS GUIDELINES:
- Vary sentence length (mix short and long sentences)
- Use contractions occasionally
- Include rhetorical questions
- Use transition words (however, moreover, additionally)
- Avoid repetitive sentence structures
- Write as if explaining to a colleague

KEYWORD USAGE:
- Use primary keyword in first 100 words
- Distribute keywords naturally throughout
- Use variations and synonyms
- Never force keywords awkwardly

OUTPUT FORMAT:
- Use proper Markdown formatting
- H1 for title only
- H2 for main sections
- H3 for subsections
- H4 for sub-subsections
- Include internal links as [anchor text](url)
- Use **bold** for emphasis sparingly
- Use lists and tables where appropriate

Write the complete blog post in Markdown format."""
    
    def _build_user_prompt(
        self, 
        outline: ContentOutline, 
        strategy_brief: StrategyBrief,
        tone: str,
        include_faq: bool,
        custom_instructions: str = None
    ) -> str:
        """Build user prompt with outline and requirements"""
        
        cluster = strategy_brief.keyword_cluster
        
        # Format sections
        sections_text = self._format_sections_for_prompt(outline.sections)
        
        prompt = f"""Write a complete, SEO-optimized blog post following this outline:

TITLE: {outline.title}
META DESCRIPTION: {outline.meta_description}
TARGET WORD COUNT: {outline.target_word_count}
TONE: {tone}

PRIMARY KEYWORD: {cluster.primary}
SECONDARY KEYWORDS: {', '.join(cluster.secondary[:8])}
LONG-TAIL KEYWORDS: {', '.join(cluster.long_tail[:8])}

INTRODUCTION:
{outline.introduction_brief}
- Must include primary keyword in first paragraph
- Hook the reader with a compelling opening
- Preview what they'll learn
- Target 150-200 words

MAIN SECTIONS:
{sections_text}

CONCLUSION:
{outline.conclusion_brief}
- Summarize key takeaways
- Include call-to-action: {outline.primary_cta or 'Take action today'}
- Reinforce primary keyword
- Target 150-200 words

{"FAQ SECTION (Add at the end before conclusion):" if include_faq else ""}
{self._generate_faq_instructions(cluster) if include_faq else ""}

INTERNAL LINKING:
Include these internal links naturally in the content:
{chr(10).join(f'- [{link.get("anchor", "related article")}]({link.get("target", "#")})' for link in outline.internal_links[:5])}

SNIPPET OPTIMIZATION PRIORITY:
Focus on these sections for featured snippet optimization:
{chr(10).join(f'- {s.heading}' for s in outline.sections if s.snippet_opportunity)}

REQUIREMENTS:
1. Write in {tone} tone
2. Use natural, human-like language
3. Include practical examples and actionable tips
4. Optimize snippet-priority sections with concise answers
5. Use varied sentence structures
6. Include transition words between sections
7. Format in clean Markdown
8. Ensure keyword density of 1-2% for primary keyword
9. Make content scannable with lists and short paragraphs
10. Write {outline.target_word_count} words minimum
{f"{chr(10)}CUSTOM INSTRUCTIONS (follow these carefully):{chr(10)}{custom_instructions}" if custom_instructions else ""}

Write the complete blog post now in Markdown format. Start with # {outline.title}"""
        
        return prompt
    
    def _format_sections_for_prompt(self, sections, level=2) -> str:
        """Format sections hierarchically for prompt"""
        formatted = []
        
        for section in sections:
            indent = "  " * (level - 2)
            marker = "#" * level
            
            formatted.append(f"{indent}{marker} {section.heading}")
            formatted.append(f"{indent}   Keywords: {', '.join(section.target_keywords[:3])}")
            formatted.append(f"{indent}   Word count: ~{section.estimated_word_count}")
            
            if section.snippet_opportunity:
                formatted.append(f"{indent}   [SNIPPET] SNIPPET OPPORTUNITY - Answer concisely in 40-60 words")
            
            formatted.append("")
            
            if section.subsections:
                formatted.append(self._format_sections_for_prompt(section.subsections, level + 1))
        
        return "\n".join(formatted)
    
    def _generate_faq_instructions(self, cluster) -> str:
        """Generate FAQ section instructions"""
        questions = cluster.related_questions[:5]
        
        faq_text = "Add an FAQ section with these questions:\n"
        for q in questions:
            faq_text += f"- {q}\n"
        faq_text += "\nAnswer each question concisely (40-60 words) for snippet optimization."
        
        return faq_text
    
    def _post_process_content(self, content: str) -> str:
        """Post-process generated content"""
        
        # Ensure single H1
        h1_count = len(re.findall(r'^#\s+', content, re.MULTILINE))
        if h1_count > 1:
            # Convert extra H1s to H2s
            lines = content.split('\n')
            h1_found = False
            processed = []
            for line in lines:
                if line.startswith('# '):
                    if h1_found:
                        processed.append('##' + line[1:])
                    else:
                        processed.append(line)
                        h1_found = True
                else:
                    processed.append(line)
            content = '\n'.join(processed)
        
        # Remove excessive blank lines
        content = re.sub(r'\n{4,}', '\n\n\n', content)
        
        # Ensure proper spacing around headings
        content = re.sub(r'(^#{1,6}\s+.+$)', r'\n\1\n', content, flags=re.MULTILINE)
        
        # Clean up
        content = content.strip()
        
        return content
    
    def _create_blog_draft(
        self, 
        markdown_content: str, 
        outline: ContentOutline,
        strategy_brief: StrategyBrief
    ) -> BlogDraft:
        """Create BlogDraft object from generated content"""
        
        # Extract title
        title_match = re.search(r'^#\s+(.+)$', markdown_content, re.MULTILINE)
        title = title_match.group(1) if title_match else outline.title
        
        # Count words (excluding markdown syntax)
        clean_text = re.sub(r'[#*`\[\]()]', '', markdown_content)
        word_count = len(clean_text.split())
        
        # Count sections
        sections_count = len(re.findall(r'^#{2,6}\s+', markdown_content, re.MULTILINE))
        
        # Extract keywords used
        content_lower = markdown_content.lower()
        keywords_used = []
        
        cluster = strategy_brief.keyword_cluster
        if cluster.primary.lower() in content_lower:
            keywords_used.append(cluster.primary)
        
        for kw in cluster.secondary[:10]:
            if kw.lower() in content_lower:
                keywords_used.append(kw)
        
        # Identify snippet-optimized sections
        snippet_sections = []
        for section in outline.sections:
            if section.snippet_opportunity and section.heading.lower() in content_lower:
                snippet_sections.append(section.heading)
        
        # Count internal links
        internal_links = len(re.findall(r'\[([^\]]+)\]\((?!http)', markdown_content))
        
        return BlogDraft(
            title=title,
            meta_description=outline.meta_description,
            markdown_content=markdown_content,
            word_count=word_count,
            sections_count=sections_count,
            keywords_used=keywords_used,
            snippet_optimized_sections=snippet_sections,
            internal_links_added=internal_links
        )
