"""
===============================================================================
Project: PACIOLI
Module: logic.domain.services.hash_counter_cache_manager
===============================================================================

Description:
    Domain service for managing the hash counter cache. It ensures robust 
    normalization of null values and provides a single point of interaction 
    for tracking transaction counters across different credit card brands.

Responsibilities:
    - Update the hash counter cache for specific accounting periods.
    - Retrieve current counters with automatic NULL-to-empty-string normalization.
    - Rebuild the cache from scratch based on historical transaction data.
    - Validate cache integrity and identify potential inconsistencies.
    - Provide cache performance and coverage statistics.

Key Components:
    - HashCounterCacheManager: Main service class for cache management.

Notes:
    - Normalizes missing batch numbers to an empty string ('') throughout.
    - Adapted for PostgreSQL syntax and schema-explicit queries.
    - Tracks occurrences and last used counters for deterministic hash generation.

Dependencies:
    - sqlalchemy, typing, datetime, utils.logger
===============================================================================
"""

from sqlalchemy import text
from typing import Dict, Tuple, Optional
from datetime import date

from utils.logger import get_logger


class HashCounterCacheManager:
    """
    Cache manager with robust NULL normalization.
    
    PRINCIPLE:
    ─────────
    Absent batch_number → '' (empty string) throughout the entire application.
    
    ARCHITECTURE:
    ────────────
    This class is the SINGLE point of contact with hash_counter_cache.
    It guarantees that NULL is ALWAYS normalized to '' during read/write operations.
    """
    
    # Shared constant for normalization
    NO_BATCH = ''
    
    def __init__(self, session):
        self.session = session
        self.logger = get_logger("HASH_CACHE_MGR")
    
    def update_cache_for_period(
        self,
        start_date: date,
        end_date: date
    ) -> Dict:
        """
        Updates the cache with NULL normalization for a specific period.
        """
        
        self.logger(
            f"Updating cache for period {start_date} to {end_date}...",
            "INFO"
        )
        
        # 1. Query Execution
        query = text("""
            INSERT INTO biq_stg.hash_counter_cache (
                brand,
                batch_number,
                amount_total,
                last_counter,
                last_updated_date,
                total_occurrences
            )
            SELECT 
                brand,
                -- NORMALIZATION: NULL → ''
                COALESCE(
                    CASE 
                        WHEN brand = 'PACIFICARD' THEN batch_number
                        ELSE NULL
                    END,
                    ''
                ) as batch_number,
                amount_total,
                MAX(
                    CAST(
                        SPLIT_PART(match_hash_key, '_', array_length(string_to_array(match_hash_key, '_'), 1))
                        AS INTEGER
                    )
                ) as last_counter,
                :end_date as last_updated_date,
                COUNT(*) as period_count
            FROM biq_stg.stg_bank_transactions
            WHERE doc_date BETWEEN :start_date AND :end_date
              AND match_hash_key ~ '_[0-9]+$'
              AND brand IS NOT NULL
              AND brand != 'NA'
              AND brand != ''
              AND doc_type = 'ZR'
            GROUP BY 
                brand,
                COALESCE(
                    CASE 
                        WHEN brand = 'PACIFICARD' THEN batch_number
                        ELSE NULL
                    END,
                    ''
                ),
                amount_total
            ON CONFLICT (brand, batch_number, amount_total) DO UPDATE SET
                last_counter = GREATEST(hash_counter_cache.last_counter, EXCLUDED.last_counter),
                last_updated_date = EXCLUDED.last_updated_date,
                total_occurrences = hash_counter_cache.total_occurrences + EXCLUDED.total_occurrences
        """)
        
        try:
            result = self.session.execute(query, {
                'start_date': start_date,
                'end_date': end_date
            })
            
            # 2. Result Compilation
            rows_affected = result.rowcount
            stats = self._get_cache_stats()
            
            self.logger(
                f"Cache updated: {rows_affected} groups processed",
                "SUCCESS"
            )
            
            return {
                'groups_updated': rows_affected,
                'total_groups_in_cache': stats['total_groups'],
                'newest_date': stats['newest_date']
            }
            
        except Exception as e:
            self.logger(f"Error updating cache: {e}", "ERROR")
            raise
    
    def get_last_counters(self) -> Dict[Tuple[str, str, float], int]:
        """
        Retrieves ALL counters with applied normalization.
        """
        
        query = text("""
            SELECT 
                brand,
                batch_number,
                amount_total,
                last_counter
            FROM biq_stg.hash_counter_cache
        """)
        
        try:
            result = self.session.execute(query)
            
            counters = {}
            for row in result:
                # 1. Apply Normalization (NULL → '')
                batch_normalized = row[1] if row[1] else self.NO_BATCH
                
                key = (
                    row[0],
                    batch_normalized,
                    float(row[2])
                )
                counters[key] = int(row[3])
            
            self.logger(
                f"Cache loaded: {len(counters)} groups",
                "INFO"
            )
            
            return counters
            
        except Exception as e:
            self.logger(f"Error querying cache: {e}", "ERROR")
            return {}
    
    def get_counter_for_key(
        self,
        brand: str,
        batch_number: Optional[str],
        amount: float
    ) -> int:
        """
        Retrieves a specific counter with normalization.
        """
        
        # 1. Normalize Input
        batch_normalized = batch_number if batch_number else self.NO_BATCH
        
        query = text("""
            SELECT last_counter
            FROM biq_stg.hash_counter_cache
            WHERE brand = :brand
              AND batch_number = :batch_number
              AND amount_total = :amount
            LIMIT 1
        """)
        
        try:
            result = self.session.execute(query, {
                'brand': brand,
                'batch_number': batch_normalized,
                'amount': amount
            }).fetchone()
            
            if result:
                return int(result[0])
            
            return 0
            
        except Exception as e:
            self.logger(
                f"Error querying counter for {brand} {batch_normalized} {amount}: {e}",
                "ERROR"
            )
            return 0
    
    def rebuild_cache_from_scratch(self) -> Dict:
        """
        Fully rebuilds the cache from transaction history.
        """
        
        self.logger("Rebuilding cache from scratch...", "WARN")
        
        # 1. Truncate existing table
        self.session.execute(text("TRUNCATE TABLE biq_stg.hash_counter_cache RESTART IDENTITY CASCADE"))
        
        # 2. Reconstruct with normalization
        query = text("""
            INSERT INTO biq_stg.hash_counter_cache (
                brand,
                batch_number,
                amount_total,
                last_counter,
                last_updated_date,
                total_occurrences
            )
            SELECT 
                brand,
                -- NORMALIZATION
                COALESCE(
                    CASE 
                        WHEN brand = 'PACIFICARD' THEN batch_number
                        ELSE NULL
                    END,
                    ''
                ) as batch_number,
                amount_total,
                MAX(
                    CAST(
                        SPLIT_PART(match_hash_key, '_', array_length(string_to_array(match_hash_key, '_'), 1))
                        AS INTEGER
                    )
                ) as last_counter,
                MAX(doc_date) as last_updated_date,
                COUNT(*) as total_occurrences
            FROM biq_stg.stg_bank_transactions
            WHERE match_hash_key ~ '_[0-9]+$'
              AND brand IS NOT NULL
              AND brand != 'NA'
              AND brand != ''
              AND doc_type = 'ZR'
            GROUP BY 
                brand,
                COALESCE(
                    CASE 
                        WHEN brand = 'PACIFICARD' THEN batch_number
                        ELSE NULL
                    END,
                    ''
                ),
                amount_total
        """)
        
        try:
            self.session.execute(query)
            
            stats = self._get_cache_stats()
            
            self.logger(
                f"Cache rebuilt: {stats['total_groups']} groups",
                "SUCCESS"
            )
            
            return stats
            
        except Exception as e:
            self.logger(f"Error rebuilding cache: {e}", "ERROR")
            raise
    
    def validate_cache_integrity(self) -> Dict:
        """Validates cache integrity against actual transaction data."""
        
        self.logger("Validating cache integrity...", "INFO")
        
        issues = []
        
        # 1. Missing Groups Validation
        query_missing = text("""
            SELECT COUNT(DISTINCT 
                brand,
                COALESCE(
                    CASE WHEN brand = 'PACIFICARD' THEN batch_number ELSE NULL END,
                    ''
                ),
                amount_total
            ) as missing_count
            FROM biq_stg.stg_bank_transactions t
            WHERE match_hash_key ~ '_[0-9]+$'
              AND brand IS NOT NULL
              AND brand != 'NA'
              AND doc_type = 'ZR'
              AND NOT EXISTS (
                  SELECT 1 
                  FROM biq_stg.hash_counter_cache c
                  WHERE c.brand = t.brand
                    AND c.batch_number = COALESCE(
                        CASE WHEN t.brand = 'PACIFICARD' THEN t.batch_number ELSE NULL END,
                        ''
                    )
                    AND c.amount_total = t.amount_total
              )
        """)
        
        missing = self.session.execute(query_missing).fetchone()[0]
        
        if missing > 0:
            issues.append(f"{missing} groups missing in cache")
        
        # 2. Inconsistent Counters Validation
        query_inconsistent = text("""
            SELECT COUNT(*) as inconsistent_count
            FROM biq_stg.hash_counter_cache c
            INNER JOIN (
                SELECT 
                    brand,
                    COALESCE(
                        CASE WHEN brand = 'PACIFICARD' THEN batch_number ELSE NULL END,
                        ''
                    ) as batch_number,
                    amount_total,
                    MAX(
                        CAST(
                            SPLIT_PART(match_hash_key, '_', array_length(string_to_array(match_hash_key, '_'), 1))
                            AS INTEGER
                        )
                    ) as max_counter
                FROM biq_stg.stg_bank_transactions
                WHERE match_hash_key ~ '_[0-9]+$'
                  AND doc_type = 'ZR'
                GROUP BY 
                    brand,
                    COALESCE(
                        CASE WHEN brand = 'PACIFICARD' THEN batch_number ELSE NULL END,
                        ''
                    ),
                    amount_total
            ) t ON c.brand = t.brand 
               AND c.batch_number = t.batch_number
               AND c.amount_total = t.amount_total
            WHERE c.last_counter < t.max_counter
        """)
        
        inconsistent = self.session.execute(query_inconsistent).fetchone()[0]
        
        if inconsistent > 0:
            issues.append(f"{inconsistent} outdated counters identified")
        
        # 3. Summary and Reporting
        is_valid = len(issues) == 0
        
        if is_valid:
            self.logger("Cache is valid", "SUCCESS")
        else:
            self.logger(f"Integrity issues found: {issues}", "WARN")
        
        return {
            'is_valid': is_valid,
            'issues': issues
        }
    
    def _get_cache_stats(self) -> Dict:
        """Retrieves performance and coverage statistics from the cache."""
        
        query = text("""
            SELECT 
                COUNT(*) as total_groups,
                SUM(total_occurrences) as total_transactions,
                MAX(last_counter) as max_counter,
                MIN(last_updated_date) as oldest_date,
                MAX(last_updated_date) as newest_date
            FROM biq_stg.hash_counter_cache
        """)
        
        result = self.session.execute(query).fetchone()
        
        return {
            'total_groups': result[0] or 0,
            'total_transactions': result[1] or 0,
            'max_counter': result[2] or 0,
            'oldest_date': result[3],
            'newest_date': result[4]
        }
    
    def get_cache_summary(self) -> Dict:
        """Returns a summarized report of current cache status."""
        
        stats = self._get_cache_stats()
        
        self.logger("Cache statistics:", "INFO")
        self.logger(f"   Unique groups: {stats['total_groups']}", "INFO")
        self.logger(f"   Total transactions: {stats['total_transactions']}", "INFO")
        self.logger(f"   Maximum counter: {stats['max_counter']}", "INFO")
        self.logger(f"   Date range: {stats['oldest_date']} to {stats['newest_date']}", "INFO")
        
        return stats
