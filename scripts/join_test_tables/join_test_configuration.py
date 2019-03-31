#!/usr/bin/env python

from enum import Enum
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from marshmallow import Schema
from marshmallow_enum import EnumField

class DataType(Enum):
    Int = 'int'
    Long = 'long'
    Float = 'float'
    Double = 'double'
    String = 'string'
    
class PredicateCondition(Enum):
    Equals = 'Equals'
    NotEquals = 'NotEquals'
    LessThan = 'LessThan'
    LessThanEquals = 'LessThanEquals'
    GreaterThan = 'GreaterThan'
    GreaterThanEquals = 'GreaterThanEquals'
    
class JoinMode(Enum):
    Inner = 'Inner'
    Left = 'Left'
    Right = 'Right'
    Full = 'FullOuter'
    Semi = 'Semi'
    AntiNullAsTrue = 'AntiNullAsTrue'
    AntiNullAsFalse = 'AntiNullAsFalse'

class ReferenceSegment(Enum):
    Yes = 'Yes'
    No = 'No'
    Join = 'Join'

import random

# All possible shapes of each dimension

all_left_table_sizes = [0, 10, 15]
all_right_table_sizes = [0, 10, 15]
all_left_nulls = [True, False]
all_right_nulls = [True, False]
all_chunk_sizes = [0, 3, 10]
all_mpj = [1, 2]
all_swap_tables = [True, False]

@dataclass_json
@dataclass
class JoinTestConfiguration(Schema):
    left_data_type: DataType = EnumField(DataType)
    right_data_type: DataType = EnumField(DataType)
    predicate_condition: PredicateCondition = EnumField(PredicateCondition)
    join_mode: JoinMode = EnumField(JoinMode)
    left_table_size: int
    right_table_size: int
    left_reference_segment: ReferenceSegment = EnumField(ReferenceSegment)
    right_reference_segment: ReferenceSegment = EnumField(ReferenceSegment)
    left_nullable: bool
    right_nullable: bool
    chunk_size: int
    mpj: int
    swap_tables: bool
    output_file_path: str = ''
        
    @staticmethod
    def get_random():
        # Avoid Joins between String and some other data type
        left_data_type: DataType = random.choice(list(DataType))
        right_data_type: DataType = random.choice(list(DataType))
        
        # XOR
        while ((left_data_type == DataType.String) != (right_data_type == DataType.String)):
            left_data_type: DataType = random.choice(list(DataType))
            right_data_type: DataType = random.choice(list(DataType))
        
        predicate_condition: PredicateCondition = random.choice(list(PredicateCondition))
        join_mode: JoinMode = random.choice(list(JoinMode))
        # only table size != 0
        left_table_size: int = random.choice([x for x in all_left_table_sizes if x != 0])
        right_table_size: int = random.choice([x for x in all_right_table_sizes if x != 0])
        left_reference_segment: ReferenceSegment = random.choice(list(ReferenceSegment))
        right_reference_segment: ReferenceSegment = random.choice(list(ReferenceSegment))
        left_null: bool = random.choice(all_left_nulls)
        right_null: bool = random.choice(all_right_nulls)
        chunk_size: int = random.choice(all_chunk_sizes)
        mpj: int = random.choice(all_mpj)
        swap_table: bool = random.choice(all_swap_tables)

        return JoinTestConfiguration(
            left_data_type, right_data_type,
            predicate_condition, join_mode,
            left_table_size, right_table_size,
            left_reference_segment, right_reference_segment,
            left_null, right_null,
            chunk_size, mpj, swap_table
        )