"""
Data Validator - Comprehensive data quality validation
"""

import logging
from typing import Dict, Any, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DataValidator:
    """Validates data quality and integrity"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.validation_rules = config.get('validation', {})
        
    def validate_schema(
        self, 
        df: DataFrame, 
        expected_schema: StructType
    ) -> Tuple[bool, List[str]]:
        """Validate DataFrame schema against expected schema"""
        errors = []
        
        # Check if all expected columns exist
        expected_cols = set(expected_schema.fieldNames())
        actual_cols = set(df.columns)
        
        missing_cols = expected_cols - actual_cols
        extra_cols = actual_cols - expected_cols
        
        if missing_cols:
            errors.append(f"Missing columns: {missing_cols}")
        if extra_cols:
            errors.append(f"Extra columns: {extra_cols}")
            
        # Check data types
        for field in expected_schema.fields:
            if field.name in df.columns:
                actual_type = df.schema[field.name].dataType
                if actual_type != field.dataType:
                    errors.append(
                        f"Type mismatch for {field.name}: "
                        f"expected {field.dataType}, got {actual_type}"
                    )
        
        is_valid = len(errors) == 0
        if is_valid:
            logger.info("Schema validation passed")
        else:
            logger.error(f"Schema validation failed: {errors}")
            
        return is_valid, errors
    
    def validate_completeness(self, df: DataFrame) -> Dict[str, Any]:
        """Check data completeness (null counts, missing values)"""
        total_rows = df.count()
        
        completeness_report = {
            'total_rows': total_rows,
            'column_completeness': {}
        }
        
        for col in df.columns:
            null_count = df.filter(F.col(col).isNull()).count()
            completeness_pct = ((total_rows - null_count) / total_rows * 100) if total_rows > 0 else 0
            
            completeness_report['column_completeness'][col] = {
                
                'null_count': null_count,
                'non_null_count': total_rows - null_count,
                'completeness_percentage': round(completeness_pct, 2)
            }
        
        logger.info(f"Completeness validation: {completeness_report}")
        return completeness_report
    
    def validate_uniqueness(self, df: DataFrame, unique_cols: List[str]) -> Dict[str, Any]:
        """Check uniqueness constraints"""
        uniqueness_report = {}
        
        for col in unique_cols:
            total_count = df.count()
            distinct_count = df.select(col).distinct().count()
            duplicate_count = total_count - distinct_count
            
            uniqueness_report[col] = {
                'total_count': total_count,
                'distinct_count': distinct_count,
                'duplicate_count': duplicate_count,
                'is_unique': duplicate_count == 0
            }
        
        logger.info(f"Uniqueness validation: {uniqueness_report}")
        return uniqueness_report
    
    def validate_ranges(self, df: DataFrame) -> Dict[str, Any]:
        """Validate numeric ranges and boundaries"""
        range_rules = self.validation_rules.get('ranges', {})
        range_report = {}
        
        for col, rules in range_rules.items():
            if col not in df.columns:
                continue
                
            stats = df.agg(
                F.min(col).alias('min'),
                F.max(col).alias('max'),
                F.count(F.when(F.col(col) < rules.get('min', float('-inf')), 1)).alias('below_min'),
                F.count(F.when(F.col(col) > rules.get('max', float('inf')), 1)).alias('above_max')
            ).collect()[0]
            
            range_report[col] = {
                'min_value': stats['min'],
                'max_value': stats['max'],
                'expected_min': rules.get('min'),
                'expected_max': rules.get('max'),
                'violations_below_min': stats['below_min'],
                'violations_above_max': stats['above_max'],
                'is_valid': stats['below_min'] == 0 and stats['above_max'] == 0
            }
        
        logger.info(f"Range validation: {range_report}")
        return range_report
    
    def validate_patterns(self, df: DataFrame) -> Dict[str, Any]:
        """Validate string patterns using regex"""
        pattern_rules = self.validation_rules.get('patterns', {})
        pattern_report = {}
        
        for col, pattern in pattern_rules.items():
            if col not in df.columns:
                continue
                
            total_count = df.filter(F.col(col).isNotNull()).count()
            matching_count = df.filter(
                F.col(col).rlike(pattern)
            ).count()
            
            pattern_report[col] = {
                'total_non_null': total_count,
                'matching_pattern': matching_count,
                'not_matching': total_count - matching_count,
                'match_percentage': round((matching_count / total_count * 100) if total_count > 0 else 0, 2),
                'pattern': pattern
            }
        
        logger.info(f"Pattern validation: {pattern_report}")
        return pattern_report
    
    def validate_referential_integrity(
        self, 
        df: DataFrame, 
        ref_col: str, 
        valid_values: List[Any]
    ) -> Dict[str, Any]:
        """Check referential integrity"""
        total_count = df.count()
        invalid_count = df.filter(
            ~F.col(ref_col).isin(valid_values)
        ).count()
        
        integrity_report = {
            'column': ref_col,
            'total_rows': total_count,
            'invalid_references': invalid_count,
            'valid_percentage': round(((total_count - invalid_count) / total_count * 100) if total_count > 0 else 0, 2),
            'is_valid': invalid_count == 0
        }
        
        logger.info(f"Referential integrity validation: {integrity_report}")
        return integrity_report
    
    def validate_business_rules(self, df: DataFrame) -> Dict[str, Any]:
        """Validate custom business rules"""
        business_rules_report = {}
        
        # Rule 1: Revenue should not be less than budget for profitable movies
        if 'revenue_musd' in df.columns and 'budget_musd' in df.columns:
            invalid_profit = df.filter(
                (F.col('revenue_musd') > 0) & 
                (F.col('budget_musd') > 0) &
                (F.col('revenue_musd') < F.col('budget_musd'))
            ).count()
            
            business_rules_report['revenue_vs_budget'] = {
                'rule': 'Revenue >= Budget for profitable movies',
                'violations': invalid_profit,
                'is_valid': invalid_profit == 0
            }
        
        # Rule 2: Vote average should be between 0 and 10
        if 'vote_average' in df.columns:
            invalid_ratings = df.filter(
                (F.col('vote_average') < 0) | 
                (F.col('vote_average') > 10)
            ).count()
            
            business_rules_report['vote_average_range'] = {
                'rule': 'Vote average between 0 and 10',
                'violations': invalid_ratings,
                'is_valid': invalid_ratings == 0
            }
        
        # Rule 3: Release date should not be in the future
        if 'release_date' in df.columns:
            from datetime import datetime
            future_dates = df.filter(
                F.col('release_date') > F.lit(datetime.now())
            ).count()
            
            business_rules_report['future_release_dates'] = {
                'rule': 'Release date not in future',
                'violations': future_dates,
                'is_valid': future_dates == 0
            }
        
        logger.info(f"Business rules validation: {business_rules_report}")
        return business_rules_report
    
    def generate_validation_report(self, df: DataFrame) -> Dict[str, Any]:
        """Generate comprehensive validation report"""
        logger.info("Generating comprehensive validation report")
        
        report = {
            'timestamp': F.current_timestamp(),
            'total_records': df.count(),
            'completeness': self.validate_completeness(df),
            'uniqueness': self.validate_uniqueness(df, ['id']),
            'ranges': self.validate_ranges(df),
            'business_rules': self.validate_business_rules(df)
        }
        
        # Calculate overall health score
        total_checks = 0
        passed_checks = 0
        
        for category, results in report.items():
            if isinstance(results, dict) and 'is_valid' in results:
                total_checks += 1
                if results['is_valid']:
                    passed_checks += 1
        
        report['health_score'] = round(
            (passed_checks / total_checks * 100) if total_checks > 0 else 0, 
            2
        )
        
        logger.info(f"Validation report generated. Health score: {report['health_score']}%")
        return report
    
    def log_data_quality_issues(self, df: DataFrame) -> None:
        """Log detailed data quality issues for investigation"""
        issues = []
        
        # Find rows with excessive nulls
        null_threshold = self.validation_rules.get('max_null_columns', 5)
        null_counts = df.select(
            [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c) 
             for c in df.columns]
        )
        
        # Find outliers in numeric columns
        numeric_cols = [
            f.name for f in df.schema.fields 
            if f.dataType.typeName() in ['integer', 'long', 'float', 'double']
        ]
        
        for col in numeric_cols:
            stats = df.select(
                F.mean(col).alias('mean'),
                F.stddev(col).alias('stddev')
            ).collect()[0]
            
            if stats['stddev']:
                outliers = df.filter(
                    (F.col(col) > stats['mean'] + 3 * stats['stddev']) |
                    (F.col(col) < stats['mean'] - 3 * stats['stddev'])
                ).count()
                
                if outliers > 0:
                    issues.append(f"{col}: {outliers} outliers detected")
        
        if issues:
            logger.warning(f"Data quality issues found: {issues}")
        else:
            logger.info("No major data quality issues detected")