# https://pypi.org/project/schema/
import schemas

# Create a standard schema
medicalInputSchema = Schema([{'first_name': And(str, len),
                         'last_name': And(str, len),
					     'age': And(Use(int), lambda n: 0 <= n <= 130),
					     'date_of_birth': And(str, len),
					     'incurred_date': And(str, len),
					     'allowed_amount': And(str, len),
					     'net_paid': And(str, len),
					     'gender': And(str, Use(str.upper), lambda s: s in ('M', 'F', 'U'))}])

medicalOutputSchema = Schema([{'full_name': And(str, len),
					           'age': And(Use(int), lambda n: 0 <= n <= 130),
					           'date_of_birth': And(date, len),
					           'incurred_date': And(date, len),
					           'allowed_amount': And(decimal(10,2), len),
					           'net_paid': And(decimal(10,2), len),
					           'gender': And(str, Use(str.upper), lambda s: s in ('M', 'F', 'U'))}])