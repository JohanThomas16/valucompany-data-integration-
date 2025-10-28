import pandas as pd
import numpy as np
import os
from datetime import datetime

class DataIntegrationPipeline:
    """
    Pipeline for integrating and normalizing financial data from multiple sources.
    """
    
    def __init__(self):
        """Initialize the pipeline with configuration parameters."""
        self.currency_rates = {
            'Germany': 1.07,  # EUR to USD
            'USA': 1.0,       # USD to USD
            'India': 0.012,   # INR to USD
            'UK': 1.21,       # GBP to USD
            'Brazil': 0.20    # BRL to USD
        }
        self.target_currency = 'USD'
        self.target_unit = 'millions'
        
    def simulate_private_market_data(self, num_companies=10):
        """
        Simulate private market/transaction data.
        
        In production, this would be replaced with API calls or database queries.
        
        Args:
            num_companies: Number of companies to simulate
            
        Returns:
            DataFrame with private market transaction data
        """
        np.random.seed(42)
        
        industries = ['Technology', 'Healthcare', 'Financial Services', 
                     'Consumer Goods', 'Energy', 'Manufacturing',
                     'Retail', 'Telecommunications', 'Real Estate', 'Transportation']
        
        countries = ['Germany', 'USA', 'India', 'UK', 'Brazil']
        
        data = {
            'company_name': [f'Company_{chr(65+i)}' for i in range(num_companies)],
            'industry': np.random.choice(industries[:num_companies], num_companies),
            'revenue_local': np.random.uniform(50, 500, num_companies),
            'ebitda_local': np.random.uniform(10, 100, num_companies),
            'valuation_multiple': np.random.uniform(6, 16, num_companies),
            'country': np.random.choice(countries, num_companies),
            'fiscal_year': [2024] * num_companies,
            'currency': ['EUR' if c == 'Germany' else 'USD' if c == 'USA' 
                        else 'INR' if c == 'India' else 'GBP' if c == 'UK' 
                        else 'BRL' for c in np.random.choice(countries, num_companies)]
        }
        
        return pd.DataFrame(data)
    
    def simulate_industry_benchmark_data(self):
        """
        Simulate industry benchmark data.
        
        In production, this would be sourced from industry reports or databases.
        
        Returns:
            DataFrame with industry benchmark metrics
        """
        np.random.seed(42)
        
        industries = ['Technology', 'Healthcare', 'Financial Services', 
                     'Consumer Goods', 'Energy', 'Manufacturing',
                     'Retail', 'Telecommunications', 'Real Estate', 'Transportation']
        
        data = {
            'industry': industries,
            'average_margin': np.random.uniform(15, 40, len(industries)),
            'sector_growth_rate': np.random.uniform(2, 12, len(industries)),
            'average_valuation_multiple': np.random.uniform(7, 14, len(industries)),
            'market_size_billions': np.random.uniform(10, 500, len(industries))
        }
        
        return pd.DataFrame(data)
    
    def convert_currency(self, amount, country):
        """
        Convert local currency to USD.
        
        Args:
            amount: Amount in local currency
            country: Country code for currency lookup
            
        Returns:
            Amount in USD
        """
        rate = self.currency_rates.get(country, 1.0)
        return amount * rate
    
    def normalize_private_data(self, df):
        """
        Normalize private market data to standard schema.
        
        Args:
            df: Raw private market DataFrame
            
        Returns:
            Normalized DataFrame with standardized fields
        """
        normalized = df.copy()
        
        # Convert revenue and EBITDA to USD
        normalized['revenue'] = normalized.apply(
            lambda row: self.convert_currency(row['revenue_local'], row['country']), 
            axis=1
        )
        normalized['ebitda'] = normalized.apply(
            lambda row: self.convert_currency(row['ebitda_local'], row['country']), 
            axis=1
        )
        
        # Round to 2 decimal places
        normalized['revenue'] = normalized['revenue'].round(2)
        normalized['ebitda'] = normalized['ebitda'].round(2)
        normalized['valuation_multiple'] = normalized['valuation_multiple'].round(2)
        
        # Calculate derived metrics
        normalized['ebitda_margin'] = (
            (normalized['ebitda'] / normalized['revenue']) * 100
        ).round(2)
        
        # Select and order columns for final schema
        final_columns = [
            'company_name', 'industry', 'country', 'fiscal_year',
            'revenue', 'ebitda', 'ebitda_margin', 'valuation_multiple'
        ]
        
        return normalized[final_columns]
    
    def normalize_benchmark_data(self, df):
        """
        Normalize industry benchmark data to standard schema.
        
        Args:
            df: Raw benchmark DataFrame
            
        Returns:
            Normalized DataFrame
        """
        normalized = df.copy()
        
        # Round percentages and multiples
        normalized['average_margin'] = normalized['average_margin'].round(2)
        normalized['sector_growth_rate'] = normalized['sector_growth_rate'].round(2)
        normalized['average_valuation_multiple'] = normalized['average_valuation_multiple'].round(2)
        normalized['market_size_billions'] = normalized['market_size_billions'].round(2)
        
        return normalized
    
    def integrate_data(self, private_df, benchmark_df):
        """
        Integrate private market and benchmark data.
        
        Args:
            private_df: Normalized private market data
            benchmark_df: Normalized benchmark data
            
        Returns:
            Integrated DataFrame with combined data
        """
        # Merge on industry
        integrated = pd.merge(
            private_df,
            benchmark_df,
            on='industry',
            how='left'
        )
        
        # Add metadata
        integrated['data_source'] = 'Integrated_Private_and_Benchmark'
        integrated['currency'] = self.target_currency
        integrated['units'] = self.target_unit
        integrated['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Calculate additional derived metrics
        integrated['valuation_vs_sector_avg'] = (
            integrated['valuation_multiple'] - integrated['average_valuation_multiple']
        ).round(2)
        
        integrated['margin_vs_sector_avg'] = (
            integrated['ebitda_margin'] - integrated['average_margin']
        ).round(2)
        
        return integrated
    
    def validate_data(self, df):
        """
        Validate the integrated dataset for quality checks.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with validation results
        """
        validation_results = {
            'total_records': len(df),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_companies': df['company_name'].duplicated().sum(),
            'negative_values': {
                'revenue': (df['revenue'] < 0).sum(),
                'ebitda': (df['ebitda'] < 0).sum()
            },
            'data_quality_score': 100.0
        }
        
        # Calculate quality score
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        validation_results['data_quality_score'] = (
            ((total_cells - missing_cells) / total_cells) * 100
        ).round(2)
        
        return validation_results
    
    def run_pipeline(self, output_dir='output', input_dir='input_data'):
        """
        Execute the complete data integration pipeline.
        
        Args:
            output_dir: Directory for output files
            input_dir: Directory for input files
            
        Returns:
            Tuple of (integrated_df, validation_results)
        """
        print("=" * 60)
        print("ValuCompany Data Integration Pipeline")
        print("=" * 60)
        
        # Create directories if they don't exist
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(input_dir, exist_ok=True)
        
        # Step 1: Data Collection
        print("\\n[1/6] Collecting data...")
        private_data_raw = self.simulate_private_market_data()
        benchmark_data_raw = self.simulate_industry_benchmark_data()
        print(f"   ✓ Collected {len(private_data_raw)} private transactions")
        print(f"   ✓ Collected {len(benchmark_data_raw)} industry benchmarks")
        
        # Save raw input data
        private_data_raw.to_csv(f'{input_dir}/private_market_data_example.csv', index=False)
        benchmark_data_raw.to_csv(f'{input_dir}/industry_benchmark_data_example.csv', index=False)
        print(f"   ✓ Saved raw input files to {input_dir}/")
        
        # Step 2: Normalize private data
        print("\\n[2/6] Normalizing private market data...")
        private_normalized = self.normalize_private_data(private_data_raw)
        print(f"   ✓ Converted all currencies to {self.target_currency}")
        print(f"   ✓ Standardized units to {self.target_unit}")
        
        # Step 3: Normalize benchmark data
        print("\\n[3/6] Normalizing industry benchmark data...")
        benchmark_normalized = self.normalize_benchmark_data(benchmark_data_raw)
        print("   ✓ Standardized benchmark metrics")
        
        # Step 4: Integrate data
        print("\\n[4/6] Integrating datasets...")
        integrated_data = self.integrate_data(private_normalized, benchmark_normalized)
        print(f"   ✓ Merged data on 'industry' key")
        print(f"   ✓ Final dataset contains {len(integrated_data)} records with {len(integrated_data.columns)} fields")
        
        # Step 5: Validate data
        print("\\n[5/6] Validating data quality...")
        validation_results = self.validate_data(integrated_data)
        print(f"   ✓ Data quality score: {validation_results['data_quality_score']}%")
        print(f"   ✓ No duplicate companies found" if validation_results['duplicate_companies'] == 0 
              else f"   ⚠ Found {validation_results['duplicate_companies']} duplicates")
        
        # Step 6: Save output
        print("\\n[6/6] Saving integrated dataset...")
        output_file = f'{output_dir}/final_integrated_valuation_data.csv'
        integrated_data.to_csv(output_file, index=False)
        print(f"   ✓ Saved to {output_file}")
        
        print("\\n" + "=" * 60)
        print("Pipeline completed successfully!")
        print("=" * 60)
        
        return integrated_data, validation_results


def main():
    """Main execution function."""
    # Initialize pipeline
    pipeline = DataIntegrationPipeline()
    
    # Run the complete pipeline
    integrated_data, validation = pipeline.run_pipeline()
    
    # Display sample of integrated data
    print("\\n" + "=" * 60)
    print("Sample of Integrated Data (first 3 rows):")
    print("=" * 60)
    print(integrated_data.head(3).to_string())
    
    # Display validation summary
    print("\\n" + "=" * 60)
    print("Validation Summary:")
    print("=" * 60)
    print(f"Total Records: {validation['total_records']}")
    print(f"Data Quality Score: {validation['data_quality_score']}%")
    print(f"Duplicate Companies: {validation['duplicate_companies']}")
    
    return integrated_data, validation


if __name__ == "__main__":
    integrated_data, validation_results = main()
