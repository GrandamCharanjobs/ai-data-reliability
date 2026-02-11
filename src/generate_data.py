import pandas as pd
import numpy as np
from faker import Faker
fake = Faker()

print("🚨 Generating 100K MESSY e-commerce orders...")
data = []
for i in range(100_000):
    data.append({
        'order_id': fake.uuid4() if np.random.rand() > 0.05 else None,
        'customer_id': fake.uuid4()[:8] if np.random.rand() > 0.02 else 'INVALID',
        'amount': round(np.random.uniform(10, 1000), 2) if np.random.rand() > 0.10 else -999,
        'timestamp': fake.date_time_this_year(),
        'email': fake.email() if np.random.rand() > 0.03 else 'broken@@email',
        'product': np.random.choice(['laptop', 'phone', 'shirt', None], p=[0.4, 0.3, 0.2, 0.1])
    })

df = pd.DataFrame(data)
df.to_csv('data/raw_orders.csv', index=False)
print("✅ SAVED: data/raw_orders.csv")
print(f"📊 Issues: {df.isnull().sum().sum()} missing values")
