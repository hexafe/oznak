"""Statistical analyzer module for performing data analysis on production data

This module provides comprehensive statistical analysis capabilities
including basic statistics quality analysis process capability calculations
and data insights generation
"""

import numpy as np
import pandas as pd
from core.session import SessionManager
import os


def execute(args: dict) -> dict:
    """Execute analyzer actions
    
    Args:
        args: Dictionary containing action and parameters
        
    Returns:
        Dictionary containing analysis results
    """
    action = args.get('action')
    
    if action == 'analyze':
        return _analyze(args)
    else:
        return {'error': f'Unknown action: {action}'}


def _analyze(args: dict) -> dict:
    """Perform statistical analysis on specified column
    
    Args:
        args: Dictionary containing analysis parameters
        
    Returns:
        Dictionary containing analysis results
    """
    try:
        session = SessionManager()
        current_source = session.get_current_source()
        
        if not current_source:
            return {'error': 'No data source selected. Use "use" command first.'}
        
        df = _load_actual_data(current_source)
        
        if df is None or df.empty:
            return {'error': 'No data available for analysis'}
        
        column = args.get('column')
        if not column:
            return {'error': 'No column specified for analysis'}
        
        if column not in df.columns:
            available_columns = list(df.columns)
            return {
                'error': f'Column "{column}" not found in dataset. Available columns: {available_columns}',
                'available_columns': available_columns
            }
        
        original_row_count = len(df)
        df = _apply_filters(df, args)
        filtered_row_count = len(df)
        
        if df.empty:
            return {'error': 'No data remaining after filtering'}
        
        data_series = df[column].dropna()
        
        if len(data_series) == 0:
            return {'error': f'No valid data in column "{column}" after removing NaN values'}
        
        data = data_series.values
        
        stats = {
            'count': len(data),
            'mean': float(np.mean(data)),
            'median': float(np.median(data)),
            'std': float(np.std(data)),
            'min': float(np.min(data)),
            'max': float(np.max(data)),
            'q25': float(np.percentile(data, 25)),
            'q75': float(np.percentile(data, 75)),
            'variance': float(np.var(data)),
            'skewness': float(pd.Series(data).skew()),
            'kurtosis': float(pd.Series(data).kurtosis())
        }
        
        quality = _perform_quality_analysis(data, args)
        insights = _generate_insights(data, column, df)
        filtering_info = {
            'original_records': original_row_count,
            'records_after_filtering': filtered_row_count,
            'records_removed_by_filtering': original_row_count - filtered_row_count
        }
        
        return {
            'column_analyzed': column,
            'statistics': stats,
            'quality': quality,
            'insights': insights,
            'filtering_info': filtering_info,
            'analyzed_records': len(data),
            'success': True
        }
        
    except Exception as e:
        return {
            'error': f'Analysis failed: {str(e)}',
            'success': False
        }


def _load_actual_data(source_info: dict) -> pd.DataFrame:
    """Load actual data based on source type
    
    Args:
        source_info: Dictionary containing source information
        
    Returns:
        DataFrame containing loaded data
    """
    source_type = source_info.get('type')
    
    if source_type == 'combined_dataset':
        data_file = source_info.get('data_file')
        if data_file and os.path.exists(data_file):
            return pd.read_parquet(data_file)
        else:
            session = SessionManager()
            try:
                return session.load_combined_dataframe()
            except FileNotFoundError:
                return pd.DataFrame()
    
    elif source_type == 'file':
        file_path = source_info.get('path')
        if file_path and os.path.exists(file_path):
            if file_path.endswith('.csv'):
                return pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                return pd.read_excel(file_path)
            elif file_path.endswith('.json'):
                return pd.read_json(file_path)
            elif file_path.endswith('.parquet'):
                return pd.read_parquet(file_path)
    
    elif source_type == 'database_table':
        pass
    
    return pd.DataFrame()


def _apply_filters(df: pd.DataFrame, args: dict) -> pd.DataFrame:
    """Apply filters to the data with configurable options
    
    Args:
        df: DataFrame to filter
        args: Dictionary containing filter parameters
        
    Returns:
        Filtered DataFrame
    """
    filtered_df = df.copy()
    
    # Load filter configuration
    session = SessionManager()
    filters_config = session.get_filters_config()
    
    # Handle date filtering
    date_column = args.get('date_column')
    date_from = args.get('date_from')
    date_to = args.get('date_to')
    
    # If no explicit dates provided, check for default
    if not date_from and not date_to and date_column:
        default_days = filters_config.get('default_date_range_days', 30)
        
        # Check if user wants all data (0 or -1)
        if default_days == 0 or default_days == -1:
            # No date filtering - use all data
            pass
        elif default_days > 0:  # Positive number = use default range
            # Apply default date range (last N days)
            cutoff_date = pd.Timestamp.now() - pd.Timedelta(days=default_days)
            if date_column in filtered_df.columns:
                # Convert to datetime if needed
                if not pd.api.types.is_datetime64_any_dtype(filtered_df[date_column]):
                    try:
                        filtered_df[date_column] = pd.to_datetime(filtered_df[date_column])
                    except Exception:
                        pass
                
                # Filter to recent data if column is datetime
                if pd.api.types.is_datetime64_any_dtype(filtered_df[date_column]):
                    filtered_df = filtered_df[filtered_df[date_column] >= cutoff_date]
    
    # Apply explicit date filters if provided (overrides defaults)
    elif date_column and date_column in filtered_df.columns:
        # Convert date column to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(filtered_df[date_column]):
            try:
                filtered_df[date_column] = pd.to_datetime(filtered_df[date_column])
            except Exception:
                pass
        
        # Apply date filters if the column is datetime
        if pd.api.types.is_datetime64_any_dtype(filtered_df[date_column]):
            if date_from:
                try:
                    date_from_parsed = pd.to_datetime(date_from)
                    filtered_df = filtered_df[filtered_df[date_column] >= date_from_parsed]
                except Exception:
                    pass  # Skip invalid date format
            
            if date_to:
                try:
                    date_to_parsed = pd.to_datetime(date_to)
                    filtered_df = filtered_df[filtered_df[date_column] <= date_to_parsed]
                except Exception:
                    pass  # Skip invalid date format
    
    # Handle value filtering
    filter_column = args.get('filter_column')
    filter_value = args.get('filter_value')
    
    if filter_column and filter_value and filter_column in filtered_df.columns:
        # Get case sensitivity setting
        case_sensitive = filters_config.get('case_sensitive_filters', False)
        
        # Try to convert filter_value to appropriate type for comparison
        sample_value = filtered_df[filter_column].iloc[0] if len(filtered_df) > 0 else None
        
        if sample_value is not None:
            try:
                # Try numeric conversion if the sample is numeric
                if pd.api.types.is_numeric_dtype(type(sample_value)):
                    numeric_value = float(filter_value)
                    filtered_df = filtered_df[filtered_df[filter_column] == numeric_value]
                else:
                    # String comparison with case sensitivity option
                    if case_sensitive:
                        filtered_df = filtered_df[filtered_df[filter_column] == filter_value]
                    else:
                        filtered_df = filtered_df[filtered_df[filter_column].str.lower() == str(filter_value).lower()]
            except (ValueError, TypeError):
                # Fallback to string comparison
                if case_sensitive:
                    filtered_df = filtered_df[filtered_df[filter_column] == str(filter_value)]
                else:
                    filtered_df = filtered_df[filtered_df[filter_column].str.lower() == str(filter_value).lower()]
    
    return filtered_df


def _perform_quality_analysis( np.ndarray, args: dict) -> dict:
    """Perform quality analysis with specification limits
    
    Args:
         Array of data values
        args: Dictionary containing quality analysis parameters
        
    Returns:
        Dictionary containing quality analysis results
    """
    quality = {}
    usl = args.get('usl')
    lsl = args.get('lsl')
    
    if usl is not None:
        out_of_spec = np.sum(data > usl)
        nok_pct = (out_of_spec / len(data)) * 100 if len(data) > 0 else 0
        quality.update({
            'usl': float(usl),
            'out_of_spec_high': int(out_of_spec),
            'nok_percentage_high': float(nok_pct),
            'within_spec_high': int(len(data) - out_of_spec)
        })
    
    if lsl is not None:
        out_of_spec = np.sum(data < lsl)
        nok_pct = (out_of_spec / len(data)) * 100 if len(data) > 0 else 0
        quality.update({
            'lsl': float(lsl),
            'out_of_spec_low': int(out_of_spec),
            'nok_percentage_low': float(nok_pct),
            'within_spec_low': int(len(data) - out_of_spec)
        })
    
    if usl is not None and lsl is not None:
        total_nok = np.sum((data > usl) | (data < lsl))
        total_ok = len(data) - total_nok
        nok_pct = (total_nok / len(data)) * 100 if len(data) > 0 else 0
        ok_pct = (total_ok / len(data)) * 100 if len(data) > 0 else 0
        
        quality['total_nok_percentage'] = float(nok_pct)
        quality['total_ok_percentage'] = float(ok_pct)
        quality['cpk'] = _calculate_cpk(data, usl, lsl)
        quality['cp'] = _calculate_cp(data, usl, lsl)
    
    return quality


def _calculate_cpk( np.ndarray, usl: float, lsl: float) -> float:
    """Calculate process capability index
    
    Args:
         Array of data values
        usl: Upper specification limit
        lsl: Lower specification limit
        
    Returns:
        Cpk value
    """
    if len(data) == 0:
        return 0.0
    
    mean_val = np.mean(data)
    std_val = np.std(data)
    
    if std_val == 0 or np.isnan(std_val):
        return float('inf') if (lsl <= mean_val <= usl) else 0.0
    
    cpu = (usl - mean_val) / (3 * std_val)
    cpl = (mean_val - lsl) / (3 * std_val)
    cpk = min(cpu, cpl)
    
    return float(cpk)


def _calculate_cp( np.ndarray, usl: float, lsl: float) -> float:
    """Calculate process capability
    
    Args:
         Array of data values
        usl: Upper specification limit
        lsl: Lower specification limit
        
    Returns:
        Cp value
    """
    if len(data) == 0:
        return 0.0
    
    std_val = np.std(data)
    
    if std_val == 0 or np.isnan(std_val):
        return float('inf')
    
    cp = (usl - lsl) / (6 * std_val)
    return float(cp)


def _generate_insights( np.ndarray, column: str, df: pd.DataFrame) -> dict:
    """Generate additional insights from the data with configurable options
    
    Args:
         Array of data values
        column: Name of the analyzed column
        df: DataFrame containing the data
        
    Returns:
        Dictionary containing insights
    """
    insights = {}
    
    # Load configuration
    session = SessionManager()
    analysis_config = session.get_analysis_config()
    quality_config = session.get_quality_config()
    
    # Configurable outlier detection
    outlier_method = analysis_config.get('outlier_method', 'iqr')
    outlier_factor = analysis_config.get('outlier_factor', 1.5)
    
    # Configurable stability thresholds
    stable_threshold = quality_config.get('stable_process_threshold', 10)
    unstable_threshold = quality_config.get('unstable_process_threshold', 25)
    
    mean_val = np.mean(data)
    median_val = np.median(data)
    std_val = np.std(data)
    
    if std_val > 0:
        skewness = (mean_val - median_val) / std_val
        if abs(skewness) < 0.5:
            insights['distribution_shape'] = 'approximately_symmetric'
        elif skewness > 0.5:
            insights['distribution_shape'] = 'right_skewed'
        else:
            insights['distribution_shape'] = 'left_skewed'
    
    # Outlier detection based on method
    if outlier_method == 'iqr':
        q75, q25 = np.percentile(data, [75, 25])
        iqr = q75 - q25
        if iqr > 0:
            lower_bound = q25 - outlier_factor * iqr
            upper_bound = q75 + outlier_factor * iqr
            outliers = np.sum((data < lower_bound) | (data > upper_bound))
        else:
            outliers = 0
    elif outlier_method == 'zscore':
        z_scores = np.abs((data - np.mean(data)) / np.std(data))
        outliers = np.sum(z_scores > outlier_factor)
    else:
        # Default IQR method
        q75, q25 = np.percentile(data, [75, 25])
        iqr = q75 - q25
        if iqr > 0:
            lower_bound = q25 - outlier_factor * iqr
            upper_bound = q75 + outlier_factor * iqr
            outliers = np.sum((data < lower_bound) | (data > upper_bound))
        else:
            outliers = 0
    
    insights['outliers_detected'] = int(outliers)
    insights['outlier_percentage'] = float((outliers / len(data)) * 100) if len(data) > 0 else 0.0
    
    if 'iqr' in locals() and iqr > 0:
        insights['outlier_bounds'] = {
            'lower': float(lower_bound),
            'upper': float(upper_bound)
        }
    
    data_range = np.max(data) - np.min(data)
    if data_range > 0:
        relative_std = (std_val / np.mean(data)) * 100 if np.mean(data) != 0 else 0
        insights['relative_standard_deviation_percent'] = float(relative_std)
        
        if relative_std < stable_threshold:
            insights['process_stability'] = 'stable'
        elif relative_std < unstable_threshold:
            insights['process_stability'] = 'moderately_stable'
        else:
            insights['process_stability'] = 'unstable'
    
    if 'production_line' in df.columns:
        line_counts = df['production_line'].value_counts()
        insights['production_line_distribution'] = line_counts.to_dict()
        
        if len(line_counts) > 1:
            line_stats = {}
            for line in line_counts.index:
                line_data = df[df['production_line'] == line][column].dropna().values
                if len(line_data) > 0:
                    line_stats[line] = {
                        'mean': float(np.mean(line_data)),
                        'std': float(np.std(line_data)),
                        'count': len(line_data)
                    }
            insights['line_performance_comparison'] = line_stats
    
    return insights

