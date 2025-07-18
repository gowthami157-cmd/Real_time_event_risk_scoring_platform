"""
Insurance claims data generator with realistic risk patterns.
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List
from faker import Faker
import json

fake = Faker()

class ClaimsGenerator:
    """Generates realistic insurance claims with varying risk patterns."""
    
    def __init__(self, seed: int = 42):
        """Initialize the claims generator with configurable seed."""
        random.seed(seed)
        fake.seed_instance(seed)
        
        # Risk patterns and distributions
        self.claim_types = {
            'auto': {
                'weight': 0.4,
                'avg_amount': 8500,
                'risk_factors': ['weather', 'location', 'driver_age', 'vehicle_age']
            },
            'property': {
                'weight': 0.3,
                'avg_amount': 15000,
                'risk_factors': ['weather', 'location', 'property_age', 'claim_history']
            },
            'health': {
                'weight': 0.2,
                'avg_amount': 5000,
                'risk_factors': ['age', 'medical_history', 'provider', 'diagnosis']
            },
            'life': {
                'weight': 0.1,
                'avg_amount': 100000,
                'risk_factors': ['age', 'health_score', 'policy_age', 'beneficiary']
            }
        }
        
        self.high_risk_locations = [
            'Miami, FL', 'New Orleans, LA', 'Houston, TX', 'Phoenix, AZ',
            'Detroit, MI', 'Newark, NJ', 'Baltimore, MD', 'Memphis, TN'
        ]
        
        self.fraud_indicators = [
            'multiple_claims_same_day', 'new_policy_immediate_claim',
            'unusual_claim_amount', 'suspicious_location', 'duplicate_documentation',
            'provider_flagged', 'beneficiary_suspicious', 'inconsistent_story'
        ]
        
        # User pools for generating repeat customers
        self.user_pool = self._generate_user_pool(10000)
        
    def _generate_user_pool(self, size: int) -> List[Dict[str, Any]]:
        """Generate a pool of users with different risk profiles."""
        users = []
        
        for _ in range(size):
            user = {
                'user_id': str(uuid.uuid4()),
                'age': random.randint(18, 85),
                'location': fake.city() + ', ' + fake.state_abbr(),
                'credit_score': random.randint(300, 850),
                'claim_history_count': random.randint(0, 15),
                'policy_tenure_years': random.randint(0, 30),
                'is_high_risk': random.random() < 0.15,  # 15% high risk users
                'risk_score': random.uniform(0.1, 0.9)
            }
            users.append(user)
        
        return users
    
    def generate_claim(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single insurance claim event."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Select user from pool
        user = random.choice(self.user_pool)
        
        # Select claim type based on weights
        claim_type = random.choices(
            list(self.claim_types.keys()),
            weights=[self.claim_types[t]['weight'] for t in self.claim_types.keys()]
        )[0]
        
        claim_info = self.claim_types[claim_type]
        
        # Generate base claim amount with realistic distribution
        base_amount = claim_info['avg_amount']
        claim_amount = max(100, random.lognormvariate(
            mean=float(base_amount), 
            sigma=0.8
        ))
        
        # Calculate risk score based on multiple factors
        risk_score = self._calculate_risk_score(user, claim_type, claim_amount, timestamp)
        
        # Generate fraud indicators if high risk
        fraud_indicators = []
        if risk_score > 0.7:
            fraud_indicators = random.choices(
                self.fraud_indicators,
                k=random.randint(1, 3)
            )
        
        claim = {
            'claim_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'timestamp': timestamp.isoformat(),
            'event_type': 'insurance_claim',
            'claim_type': claim_type,
            'claim_amount': round(claim_amount, 2),
            'location': user['location'],
            'risk_score': round(risk_score, 3),
            
            # User attributes
            'user_age': user['age'],
            'user_credit_score': user['credit_score'],
            'user_claim_history': user['claim_history_count'],
            'policy_tenure_years': user['policy_tenure_years'],
            
            # Claim specifics
            'claim_description': fake.sentence(nb_words=8),
            'adjuster_assigned': fake.name(),
            'estimated_processing_days': random.randint(5, 45),
            
            # Risk factors
            'weather_conditions': random.choice(['clear', 'rain', 'snow', 'storm', 'fog']),
            'time_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'is_holiday': self._is_holiday(timestamp),
            
            # Fraud indicators
            'fraud_indicators': fraud_indicators,
            'fraud_probability': round(min(risk_score + random.uniform(-0.1, 0.1), 1.0), 3),
            
            # Processing metadata
            'priority': 'high' if risk_score > 0.8 else 'medium' if risk_score > 0.5 else 'low',
            'requires_investigation': risk_score > 0.75,
            'auto_approved': risk_score < 0.3 and claim_amount < 5000,
            
            # Additional features for ML
            'claim_complexity_score': random.uniform(0.1, 1.0),
            'documentation_completeness': random.uniform(0.5, 1.0),
            'similar_claims_last_30_days': random.randint(0, 5),
            'provider_reputation_score': random.uniform(0.3, 1.0),
        }
        
        return claim
    
    def _calculate_risk_score(self, user: Dict[str, Any], claim_type: str, 
                            claim_amount: float, timestamp: datetime) -> float:
        """Calculate risk score based on multiple factors."""
        base_risk = user['risk_score']
        
        # Adjust based on claim amount (higher amounts = higher risk)
        amount_factor = min(claim_amount / 50000, 1.0) * 0.2
        
        # Adjust based on user history
        history_factor = min(user['claim_history_count'] / 10, 1.0) * 0.15
        
        # Adjust based on location
        location_factor = 0.2 if user['location'] in self.high_risk_locations else 0.0
        
        # Adjust based on timing (night claims slightly riskier)
        time_factor = 0.1 if timestamp.hour < 6 or timestamp.hour > 22 else 0.0
        
        # Adjust based on credit score (lower score = higher risk)
        credit_factor = max(0, (700 - user['credit_score']) / 400) * 0.1
        
        # New policy immediate claim (very high risk)
        new_policy_factor = 0.3 if user['policy_tenure_years'] < 1 else 0.0
        
        # Combine all factors
        risk_score = base_risk + amount_factor + history_factor + location_factor + \
                    time_factor + credit_factor + new_policy_factor
        
        # Add some randomness
        risk_score += random.uniform(-0.05, 0.05)
        
        return max(0.0, min(1.0, risk_score))
    
    def _is_holiday(self, timestamp: datetime) -> bool:
        """Check if timestamp falls on a major holiday."""
        # Simplified holiday detection
        holidays = [
            (1, 1),   # New Year's Day
            (7, 4),   # Independence Day
            (12, 25), # Christmas
            (11, 26), # Thanksgiving (approximation)
        ]
        
        return (timestamp.month, timestamp.day) in holidays
    
    def generate_batch(self, batch_size: int, 
                      start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate a batch of claims with realistic temporal distribution."""
        if start_time is None:
            start_time = datetime.utcnow()
        
        claims = []
        
        for i in range(batch_size):
            # Add some temporal variation
            timestamp = start_time + timedelta(
                seconds=random.randint(0, 60),
                microseconds=random.randint(0, 999999)
            )
            
            claim = self.generate_claim(timestamp)
            claims.append(claim)
        
        return claims
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the JSON schema for generated claims."""
        return {
            "type": "object",
            "properties": {
                "claim_id": {"type": "string"},
                "user_id": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
                "event_type": {"type": "string"},
                "claim_type": {"type": "string"},
                "claim_amount": {"type": "number"},
                "location": {"type": "string"},
                "risk_score": {"type": "number"},
                "user_age": {"type": "integer"},
                "user_credit_score": {"type": "integer"},
                "user_claim_history": {"type": "integer"},
                "policy_tenure_years": {"type": "integer"},
                "fraud_indicators": {"type": "array", "items": {"type": "string"}},
                "fraud_probability": {"type": "number"},
                "priority": {"type": "string"},
                "requires_investigation": {"type": "boolean"},
                "auto_approved": {"type": "boolean"}
            },
            "required": ["claim_id", "user_id", "timestamp", "event_type", "claim_type", "claim_amount", "risk_score"]
        }