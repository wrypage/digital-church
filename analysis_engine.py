import json
import re
import numpy as np

class DigitalPulpitBrain:
    def __init__(self, config_path='digital_pulpit_config.json'):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # 1. Pre-compile Patterns & Metadata
        self.tag_patterns = {}
        self.tag_metadata = {}
        self.gospel_anchors = set(t.lower() for t in self.config['theological_brain']['L1_Soteriology'])
        
        # Define boost categories for targeted multipliers (Refinement 1)
        self.verse_boost_layers = {"L1_Soteriology", "L3_Christology", "L5_Reformed_Posture", "L8_Epistemology"}
        self.imp_boost_layers = {"L1_Soteriology", "L10_Spiritual_Practices"}

        for layer, tags in self.config['theological_brain'].items():
            layer_weight = self.config['weighting_logic']['layer_weights'].get(layer, 1.0)
            for tag in tags:
                tag_lower = tag.lower()
                self.tag_patterns[tag_lower] = re.compile(r'\b' + re.escape(tag_lower) + r'\b')
                
                # Store final base weight + layer metadata for targeted boosting
                self.tag_metadata[tag_lower] = {
                    "weight": self.config['weighting_logic']['tag_overrides'].get(tag, layer_weight),
                    "layer": layer
                }

        self.multipliers = {k.lower(): v for k, v in self.config['weighting_logic']['multipliers'].items()}

    def analyze_sermon(self, transcript_segments, duration_seconds=None):
        total_weighted_score = 0
        
        # Safe duration extraction (Refinement 5)
        if not duration_seconds and transcript_segments:
            ends = [s.get("end") for s in transcript_segments if isinstance(s.get("end"), (int, float))]
            duration_seconds = max(ends) if ends else 0
        
        total_minutes = (duration_seconds / 60) if duration_seconds > 0 else 1

        for i, segment in enumerate(transcript_segments):
            # Normalize whitespace for multi-word tags (Refinement 4)
            text = re.sub(r"\s+", " ", segment['text'].lower())
            segment_score = 0
            
            # Detect Multipliers
            m_verse = self.multipliers.get('verse_citation_match', 1.0) if segment.get('verse_citation_match') else 1.0
            m_imp = self.multipliers.get('imperative_language_match', 1.0) if segment.get('imperative_language_match') else 1.0
            
            # Implement Gospel Anchor Proximity (Refinement 2)
            # Checks current, previous, and next segment for an anchor term
            context_window = [
                text,
                transcript_segments[i-1]['text'].lower() if i > 0 else "",
                transcript_segments[i+1]['text'].lower() if i < len(transcript_segments)-1 else ""
            ]
            has_anchor = any(anchor in " ".join(context_window) for anchor in self.gospel_anchors)
            m_gospel = self.multipliers.get('gospel_anchor_proximity', 1.0) if has_anchor else 1.0
            
            for tag, pattern in self.tag_patterns.items():
                # Quick pre-filter for performance (Refinement 3)
                if tag not in text: continue
                
                matches = len(pattern.findall(text))
                if matches > 0:
                    meta = self.tag_metadata[tag]
                    dampened_count = (1 + np.log(matches))
                    
                    # Targeted Multiplier Logic (Refinement 1)
                    v_boost = m_verse if meta['layer'] in self.verse_boost_layers else 1.0
                    i_boost = m_imp if meta['layer'] in self.imp_boost_layers else 1.0
                    
                    # Final hit calculation including Gospel Anchor
                    tag_hit_score = (meta['weight'] * dampened_count) * v_boost * i_boost * m_gospel
                    segment_score += tag_hit_score
            
            total_weighted_score += segment_score

        return total_weighted_score / total_minutes