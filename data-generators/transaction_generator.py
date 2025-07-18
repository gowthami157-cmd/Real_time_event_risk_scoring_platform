"""
Financial transaction data generator with fraud patterns.
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, List
from faker import Faker
import json

fake = Faker()

class TransactionGenerator:
    """Generates realistic financial transactions with fraud patterns."""
    
    def __init__(self, seed: int = 42):
        """Initialize the transaction generator."""
        random.seed(seed)
        fake.seed_instance(seed)
        
        # Transaction categories and their typical amounts
        self.transaction_categories = {
            'groceries': {'weight': 0.15, 'avg_amount': 75, 'risk_level': 'low'},
            'gas': {'weight': 0.12, 'avg_amount': 45, 'risk_level': 'low'},
            'restaurant': {'weight': 0.10, 'avg_amount': 35, 'risk_level': 'low'},
            'retail': {'weight': 0.08, 'avg_amount': 120, 'risk_level': 'low'},
            'online_shopping': {'weight': 0.07, 'avg_amount': 85, 'risk_level': 'medium'},
            'transfer': {'weight': 0.06, 'avg_amount': 500, 'risk_level': 'medium'},
            'atm_withdrawal': {'weight': 0.05, 'avg_amount': 200, 'risk_level': 'medium'},
            'travel': {'weight': 0.04, 'avg_amount': 300, 'risk_level': 'high'},
            'luxury': {'weight': 0.03, 'avg_amount': 800, 'risk_level': 'high'},
            'crypto': {'weight': 0.02, 'avg_amount': 1000, 'risk_level': 'high'},
            'gambling': {'weight': 0.01, 'avg_amount': 200, 'risk_level': 'high'},
            'cash_advance': {'weight': 0.01, 'avg_amount': 400, 'risk_level': 'high'},
        }
        
        # Fraud patterns
        self.fraud_patterns = {
            'card_testing': {'amount_range': (1, 10), 'frequency': 'high'},
            'account_takeover': {'amount_range': (100, 2000), 'frequency': 'medium'},
            'synthetic_identity': {'amount_range': (500, 5000), 'frequency': 'low'},
            'bust_out': {'amount_range': (1000, 10000), 'frequency': 'low'},
            'velocity_abuse': {'amount_range': (50, 500), 'frequency': 'high'},
        }
        
        # High-risk locations and merchants
        self.high_risk_locations = [
            'Nigeria', 'Romania', 'Russia', 'China', 'Pakistan',
            'Indonesia', 'Philippines', 'Vietnam', 'Bangladesh'
        ]
        
        self.suspicious_merchants = [
            'UNKNOWN MERCHANT', 'TEMP MERCHANT', 'CASH ADVANCE',
            'MONEY TRANSFER', 'GIFT CARD PURCHASE', 'PREPAID CARD'
        ]
        
        # Generate user pool with different spending patterns
        self.user_pool = self._generate_user_pool(20000)
    
    def _generate_user_pool(self, size: int) -> List[Dict[str, Any]]:
        """Generate a pool of users with different spending patterns."""
        users = []
        
        for _ in range(size):
            user = {
                'user_id': str(uuid.uuid4()),
                'age': random.randint(18, 85),
                'income_level': random.choice(['low', 'medium', 'high', 'very_high']),
                'location': fake.country(),
                'account_age_days': random.randint(30, 3650),
                'credit_limit': random.randint(1000, 50000),
                'avg_monthly_spending': random.randint(500, 5000),
                'transaction_history_count': random.randint(50, 5000),
                'is_fraudster': random.random() < 0.02,  # 2% fraudulent users
                'risk_profile': random.choice(['low', 'medium', 'high']),
                'spending_pattern': random.choice(['conservative', 'moderate', 'aggressive']),
                'preferred_categories': random.sample(
                    list(self.transaction_categories.keys()), 
                    random.randint(3, 6)
                )
            }
            users.append(user)
        
        return users
    
    def generate_transaction(self, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate a single financial transaction."""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Select user from pool
        user = random.choice(self.user_pool)
        
        # Determine if this will be a fraudulent transaction
        is_fraud = user['is_fraudster'] or random.random() < 0.005  # 0.5% base fraud rate
        
        if is_fraud:
            transaction = self._generate_fraudulent_transaction(user, timestamp)
        else:
            transaction = self._generate_legitimate_transaction(user, timestamp)
        
        # Add common fields
        transaction.update({
            'transaction_id': str(uuid.uuid4()),
            'user_id': user['user_id'],
            'timestamp': timestamp.isoformat(),
            'event_type': 'financial_transaction',
            'user_age': user['age'],
            'user_income_level': user['income_level'],
            'user_location': user['location'],
            'account_age_days': user['account_age_days'],
            'credit_limit': user['credit_limit'],
            'transaction_history_count': user['transaction_history_count'],
            'is_weekend': timestamp.weekday() >= 5,
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),
        })
        
        # Calculate risk score
        transaction['risk_score'] = self._calculate_risk_score(transaction, user)
        
        return transaction
    
    def _generate_legitimate_transaction(self, user: Dict[str, Any], 
                                       timestamp: datetime) -> Dict[str, Any]:
        """Generate a legitimate transaction based on user profile."""
        # Select category based on user preferences and weights
        category = random.choices(
            user['preferred_categories'],
            k=1
        )[0]
        
        category_info = self.transaction_categories[category]
        
        # Generate amount based on category and user income
        base_amount = category_info['avg_amount']
        income_multiplier = {
            'low': 0.7, 'medium': 1.0, 'high': 1.5, 'very_high': 2.0
        }[user['income_level']]
        
        amount = max(1, random.lognormvariate(
            mean=float(base_amount * income_multiplier),
            sigma=0.6
        ))
        
        return {
            'amount': round(amount, 2),
            'category': category,
            'merchant_name': fake.company(),
            'merchant_category': category,
            'location': user['location'],
            'payment_method': random.choice(['credit_card', 'debit_card', 'bank_transfer']),
            'is_online': random.random() < 0.4,
            'is_fraud': False,
            'fraud_type': None,
            'decline_reason': None,
            'authorization_code': fake.bothify(text='????##'),
            'processing_time_ms': random.randint(50, 300),
        }
    
    def _generate_fraudulent_transaction(self, user: Dict[str, Any], 
                                       timestamp: datetime) -> Dict[str, Any]:
        """Generate a fraudulent transaction."""
        # Select fraud pattern
        fraud_type = random.choice(list(self.fraud_patterns.keys()))
        fraud_info = self.fraud_patterns[fraud_type]
        
        # Generate amount based on fraud pattern
        min_amount, max_amount = fraud_info['amount_range']
        amount = random.uniform(min_amount, max_amount)
        
        # Select high-risk category
        risky_categories = [cat for cat, info in self.transaction_categories.items() 
                          if info['risk_level'] in ['medium', 'high']]
        category = random.choice(risky_categories)
        
        # Often use suspicious merchants or locations
        merchant_name = random.choice(self.suspicious_merchants + [fake.company()])
        location = random.choice(self.high_risk_locations + [user['location']])
        
        return {
            'amount': round(amount, 2),
            'category': category,
            'merchant_name': merchant_name,
            'merchant_category': category,
            'location': location,
            'payment_method': random.choice(['credit_card', 'debit_card']),
            'is_online': random.random() < 0.8,  # Fraud more likely online
            'is_fraud': True,
            'fraud_type': fraud_type,
            'decline_reason': None,
            'authorization_code': fake.bothify(text='????##'),
            'processing_time_ms': random.randint(20, 100),  # Faster processing
        }
    
    def _calculate_risk_score(self, transaction: Dict[str, Any], 
                            user: Dict[str, Any]) -> float:
        """Calculate risk score for the transaction."""
        risk_score = 0.0
        
        # Base risk from fraud flag
        if transaction['is_fraud']:
            risk_score += 0.7
        
        # Amount-based risk
        if transaction['amount'] > user['credit_limit'] * 0.8:
            risk_score += 0.2
        elif transaction['amount'] > user['avg_monthly_spending'] * 0.5:
            risk_score += 0.1
        
        # Location risk
        if transaction['location'] in self.high_risk_locations:
            risk_score += 0.15
        
        # Merchant risk
        if transaction['merchant_name'] in self.suspicious_merchants:
            risk_score += 0.2
        
        # Time-based risk
        if transaction['hour_of_day'] < 6 or transaction['hour_of_day'] > 22:
            risk_score += 0.05
        
        # Online transaction risk
        if transaction['is_online']:
            risk_score += 0.05
        
        # New account risk
        if user['account_age_days'] < 90:
            risk_score += 0.1
        
        # Category risk
        category_risk = {
            'low': 0.0, 'medium': 0.05, 'high': 0.1
        }[self.transaction_categories[transaction['category']]['risk_level']]
        risk_score += category_risk
        
        # Add some randomness
        risk_score += random.uniform(-0.02, 0.02)
        
        return max(0.0, min(1.0, risk_score))
    
    def generate_batch(self, batch_size: int, 
                      start_time: datetime = None) -> List[Dict[str, Any]]:
        """Generate a batch of transactions with realistic temporal distribution."""
        if start_time is None:
            start_time = datetime.utcnow()
        
        transactions = []
        
        for i in range(batch_size):
            # Add temporal variation
            timestamp = start_time + timedelta(
                seconds=random.randint(0, 60),
                microseconds=random.randint(0, 999999)
            )
            
            transaction = self.generate_transaction(timestamp)
            transactions.append(transaction)
        
        return transactions
    
    def get_schema(self) -> Dict[str, Any]:
        """Get the JSON schema for generated transactions."""
        return {
            "type": "object",
            "properties": {
                "transaction_id": {"type": "string"},
                "user_id": {"type": "string"},
                "timestamp": {"type": "string", "format": "date-time"},
                "event_type": {"type": "string"},
                "amount": {"type": "number"},
                "category": {"type": "string"},
                "merchant_name": {"type": "string"},
                "location": {"type": "string"},
                "payment_method": {"type": "string"},
                "is_online": {"type": "boolean"},
                "is_fraud": {"type": "boolean"},
                "fraud_type": {"type": "string"},
                "risk_score": {"type": "number"},
                "user_age": {"type": "integer"},
                "account_age_days": {"type": "integer"},
                "processing_time_ms": {"type": "integer"}
            },
            "required": ["transaction_id", "user_id", "timestamp", "event_type", "amount", "risk_score"]
        }