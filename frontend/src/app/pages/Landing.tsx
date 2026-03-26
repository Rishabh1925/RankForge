import { useState, useEffect } from "react";
import { Link } from "react-router";
import { ArrowRight, Zap, BarChart3, Shield, Brain, Target, FileText } from "lucide-react";
import { Button } from "../components/ui/button";
import { GlobalBackground } from "../components/GlobalBackground";
import { AuthModal } from "../components/AuthModal";
import { getStoredUser, isAuthenticated, logout } from "../services/auth";
import type { AuthUser } from "../services/auth";

export function LandingPage() {
  const [authModalOpen, setAuthModalOpen] = useState(false);
  const [user, setUser] = useState<AuthUser | null>(null);

  useEffect(() => {
    if (isAuthenticated()) {
      setUser(getStoredUser());
    }
  }, []);

  const features = [
    { icon: Brain, title: "Multi-Agent Pipeline", desc: "Researcher, Writer, and Editor agents work in sequence to produce quality content." },
    { icon: Target, title: "SERP Gap Analysis", desc: "Identifies missing topics and underserved questions your competitors aren't covering." },
    { icon: BarChart3, title: "Traffic Projection", desc: "Estimates monthly searches, ranking probability, and projected traffic for your keywords." },
    { icon: Shield, title: "SEO Validation", desc: "Scores content across 20+ metrics including keyword density, readability, and snippet readiness." },
    { icon: FileText, title: "Naturalness Analysis", desc: "Measures AI detection risk, sentence variety, and vocabulary richness for human-like output." },
    { icon: Zap, title: "Under 3 Minutes", desc: "Full blog generation including research, writing, and validation in a single pipeline run." },
  ];

  const metrics = [
    { value: "85–95%", label: "SEO Score" },
    { value: "< 3 min", label: "Per Blog" },
    { value: "20+", label: "Quality Metrics" },
    { value: "99.9%", label: "Uptime (Render)" },
  ];

  return (
    <div className="min-h-screen" style={{ background: "#FAF8F5" }}>
      <GlobalBackground />

      {/* Nav */}
      <nav className="relative z-10 flex items-center justify-between px-6 lg:px-16 py-5" style={{ background: "transparent" }}>
        <div className="flex items-center gap-2">
          <Zap className="w-5 h-5 text-[--text-primary]" />
          <span className="text-lg font-bold tracking-tight text-[--text-primary]">RankForge</span>
        </div>
        <div className="hidden md:flex absolute left-1/2 -translate-x-1/2 items-center gap-8">
          <button
            onClick={() => document.getElementById('features')?.scrollIntoView({ behavior: 'smooth' })}
            className="text-sm text-[--text-secondary] hover:text-[--text-primary] transition-colors"
          >Features</button>
          <button
            onClick={() => document.getElementById('metrics')?.scrollIntoView({ behavior: 'smooth' })}
            className="text-sm text-[--text-secondary] hover:text-[--text-primary] transition-colors"
          >Metrics</button>
        </div>
        <div className="flex items-center gap-3">
          {user ? (
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-full bg-[#2D2B55] flex items-center justify-center text-white text-sm font-medium">
                {user.name.charAt(0).toUpperCase()}
              </div>
              <span className="hidden md:block text-sm font-medium text-[--text-primary]">{user.name}</span>
              <button
                onClick={() => { logout(); setUser(null); }}
                className="text-sm text-[--text-secondary] hover:text-[--text-primary] transition-colors"
              >
                Logout
              </button>
            </div>
          ) : (
            <button
              onClick={() => setAuthModalOpen(true)}
              className="text-sm font-medium text-[--text-primary] hover:text-[#2D2B55] transition-colors"
            >
              Login / Signup
            </button>
          )}
          <Link to="/app">
            <Button className="btn-primary text-sm px-5 py-2 rounded-lg">
              Get Started
            </Button>
          </Link>
        </div>
      </nav>

      <AuthModal
        isOpen={authModalOpen}
        onClose={() => setAuthModalOpen(false)}
        onAuth={(u) => setUser(u)}
      />

      {/* Hero */}
      <section className="relative z-10 px-6 lg:px-16 pt-20 pb-24 max-w-5xl mx-auto text-center">
        <div className="inline-block px-4 py-1.5 rounded-full border border-[--border-subtle] text-xs font-medium text-[--text-secondary] mb-8 bg-white">
          AI-Powered Content Engine
        </div>

        <h1 className="text-5xl lg:text-7xl font-bold tracking-tight text-[--text-primary] mb-6 leading-[1.1]">
          Forge Content<br />
          <span className="text-[--text-tertiary]">That Ranks.</span>
        </h1>

        <p className="text-lg text-[--text-secondary] max-w-2xl mx-auto mb-10 leading-relaxed">
          A three-phase AI pipeline that researches, writes, and validates
          SEO-optimized blog content - in under 3 minutes, with 20+ quality
          metrics to back every output.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center">
          <Link to="/app/generate">
            <Button className="btn-primary px-8 py-6 text-base rounded-xl">
              Start Generating
              <ArrowRight className="ml-2 w-4 h-4" />
            </Button>
          </Link>
          <Link to="/app">
            <Button variant="outline" className="px-8 py-6 text-base rounded-xl border-[--border-subtle] text-[--text-primary]">
              View Dashboard
            </Button>
          </Link>
        </div>
      </section>

      {/* Metrics Bar */}
      <section id="metrics" className="relative z-10 px-6 lg:px-16 pb-20">
        <div className="max-w-4xl mx-auto grid grid-cols-2 lg:grid-cols-4 gap-6">
          {metrics.map((m, i) => (
            <div key={i} className="text-center p-6 bg-white rounded-xl border border-[--border-subtle]">
              <div className="text-3xl font-bold text-[--text-primary] mb-1">{m.value}</div>
              <div className="text-sm text-[--text-secondary]">{m.label}</div>
            </div>
          ))}
        </div>
      </section>

      {/* Features */}
      <section id="features" className="relative z-10 px-6 lg:px-16 pb-24">
        <div className="max-w-5xl mx-auto">
          <h2 className="text-3xl font-bold text-[--text-primary] text-center mb-4 tracking-tight">How It Works</h2>
          <p className="text-center text-[--text-secondary] mb-12 max-w-xl mx-auto">
            Three AI agents collaborate to produce publication-ready content, validated against real SEO benchmarks.
          </p>

          <div className="grid md:grid-cols-2 lg:grid-cols-3 gap-6">
            {features.map((f, i) => {
              const Icon = f.icon;
              return (
                <div key={i} className="p-6 bg-white rounded-xl border border-[--border-subtle] hover:border-[--border-active] transition-all duration-200">
                  <div className="w-10 h-10 rounded-lg bg-[--bg-tertiary] flex items-center justify-center mb-4">
                    <Icon className="w-5 h-5 text-[--text-primary]" />
                  </div>
                  <h3 className="text-base font-semibold text-[--text-primary] mb-2">{f.title}</h3>
                  <p className="text-sm text-[--text-secondary] leading-relaxed">{f.desc}</p>
                </div>
              );
            })}
          </div>
        </div>
      </section>

      <section className="relative z-10 px-6 lg:px-16 pb-20">
        <div className="max-w-3xl mx-auto text-center p-12 bg-accent-fill rounded-2xl">
          <h2 className="text-3xl font-bold text-white mb-4 tracking-tight">Ready to rank higher?</h2>
          <p className="text-white/70 mb-8 max-w-lg mx-auto">
            Start generating SEO-optimized blog content in minutes with the power of multi-agent AI.
          </p>
          <Link to="/app/generate">
            <Button className="bg-white text-[--accent-primary] hover:bg-white/90 px-8 py-6 text-base rounded-xl font-semibold">
              Get Started - It's Free
              <ArrowRight className="ml-2 w-4 h-4" />
            </Button>
          </Link>
        </div>
      </section>

      {/* Footer */}
      <footer className="relative z-10 px-6 lg:px-16 py-8 border-t border-[--border-subtle]">
        <div className="max-w-5xl mx-auto flex flex-col md:flex-row justify-between items-center gap-4 text-sm text-[--text-tertiary]">
          <span>© 2026 RankForge</span>
          <div className="flex gap-6">
            <a href="https://github.com/Rishabh1925/RankForge" target="_blank" rel="noopener noreferrer" className="hover:text-[--text-primary] transition-colors">GitHub</a>
            <Link to="/app" className="hover:text-[--text-primary] transition-colors">App</Link>
          </div>
        </div>
      </footer>
    </div>
  );
}
