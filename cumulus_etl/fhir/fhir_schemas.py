"""Manage FHIR resource schemas in certain formats"""

from collections import namedtuple
from functools import partial

from fhirclient.models import fhirabstractbase, fhirdate, fhirelementfactory
import pyspark


FhirProperty = namedtuple("FhirProperty", ["name", "json_name", "pytype", "is_list", "of_many", "required"])


def create_spark_schema_for_resource(resource_type: str) -> pyspark.sql.types.StructType:
    """
    Creates a Pyspark StructType schema based off the named resource (like 'Observation').

    Note that this schema will not be deep (fully nested all the way down),
    it will simply be wide (covering each toplevel field, each likely nullable).

    The primary goal here is to simplify complexity in the consuming SQL so that it
    can assume each column is at least defined.
    """
    instance = fhirelementfactory.FHIRElementFactory.instantiate(resource_type, None)
    return fhir_obj_to_pyspark_fields(instance, recurse=True)


def fhir_obj_to_pyspark_fields(
    base_obj: fhirabstractbase.FHIRAbstractBase, *, recurse: bool
) -> pyspark.sql.types.StructType:
    """Convert a FHIR instance to a Pyspark StructType schema definition"""
    properties = map(FhirProperty._make, base_obj.elementProperties())
    return pyspark.sql.types.StructType(
        list(filter(None, map(partial(fhir_to_spark_property, recurse=recurse), properties)))
    )


def fhir_to_spark_property(prop: FhirProperty, *, recurse: bool) -> pyspark.sql.types.StructField | None:
    """Converts a single FhirProperty to a Pyspark StructField, returning None if this field should be skipped"""
    spark_type = fhir_to_spark_type(prop.pytype, recurse=recurse)
    if spark_type is None:
        return None

    # Wrap lists in an ArrayType
    if prop.is_list:
        spark_type = pyspark.sql.types.ArrayType(spark_type)

    # Mark all types as nullable, don't worry about the prop.required field.
    # The ETL itself doesn't need to be in the business of validation, we just want to push the data through.
    return pyspark.sql.types.StructField(prop.json_name, spark_type, nullable=True)


def fhir_to_spark_type(pytype: type, recurse=False) -> pyspark.sql.types.DataType | None:
    """Converts a basic python type to a Pyspark type, returning None if this element should be skipped"""
    if pytype is int:
        # TODO: investigate if we can reduce this to IntegerType (32-bit) safely.
        #  The FHIR spec is 32-bit only (but does include some unsigned variants), while LongType is 64-bit.
        #  I've started this off as LongType because that matches the historical inferred types before we used a
        #  pre-calculated schema.
        return pyspark.sql.types.LongType()
    elif pytype is float:
        # TODO: the FHIR spec suggests that double might not even be enough:
        #  From https://www.hl7.org/fhir/R4/datatypes.html:
        #  "In object code, implementations that might meet this constraint are GMP implementations or equivalents
        #   to Java BigDecimal that implement arbitrary precision, or a combination of a (64 bit) floating point
        #   value with a precision field"
        #  But for now, we are matching the inferred types from before we used a pre-calculated schema.
        #  We can presumably up-scale this at some point if we find limitations.
        return pyspark.sql.types.DoubleType()
    elif pytype is str:
        return pyspark.sql.types.StringType()
    elif pytype is bool:
        return pyspark.sql.types.BooleanType()
    elif pytype is fhirdate.FHIRDate:
        return pyspark.sql.types.StringType()  # just leave it as a string, like it appears in the JSON
    elif issubclass(pytype, fhirabstractbase.FHIRAbstractBase):
        if recurse:
            return fhir_obj_to_pyspark_fields(pytype(), recurse=False)

        # Else skip this element entirely and do not descend, to avoid infinite recursion.
        # Note that in theory this might leave a struct with no child fields (if a struct's only children where also
        # structs), which parquet/spark would have an issue with -- it won't allow empty structs.
        # But in practice with FHIR, all BackboneElements have at least an id (string) field, so we dodge that bullet.
        return None
    raise ValueError(f"Unexpected type: {pytype}")
