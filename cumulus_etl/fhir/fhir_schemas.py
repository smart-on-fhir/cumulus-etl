"""Manage FHIR resource schemas in certain formats"""

from collections import namedtuple
from functools import partial
from typing import Any

import pyarrow
from fhirclient.models import fhirabstractbase, fhirdate, fhirelementfactory


FhirProperty = namedtuple("FhirProperty", ["name", "json_name", "pytype", "is_list", "of_many", "required"])


def pyarrow_schema_from_resource_batch(resource_type: str, batch: list[dict]) -> pyarrow.Schema:
    """
    Creates a PyArrow schema based off the named resource (like 'Observation') and batch contents.

    Note that this schema will not be deep (fully nested all the way down),
    it will simply be wide (covering each toplevel field, each likely nullable).
    But it *will* at least include every field contained in the batch.

    The primary goal here is to simplify complexity in the consuming SQL so that it
    can assume each column is at least defined.
    """
    # Examine batch to see the full shape of it, in order to detect any deeply nested fields that we want to make sure
    # to include in the final schema (normally, we go wide but only as deep as we need to)
    batch_shape = _get_shape_of_dicts(None, batch)

    return create_pyarrow_schema_for_resource(resource_type, batch_shape)


def _get_shape_of_dicts(total_shape: dict | None, item: Any) -> dict:
    """
    Examines `item` and gives a description of its "shape".

    Shape here means a dictionary tree of fields, like {"id": {}, "code": {"text": {}}}
    where empty dictionaries indicate no further children.

    This is not a generic concept at all - it's purely to aid with creating a schema for a batch of input rows.
    This shape will tell us which FHIR fields to include in our schema.

    Example Input:
    {"address": [{"street": "123 Main St", "city": "Springfield"}], "name": "Jane Smith"}

    Example output:
    {"address": {"street": {}, "city": {}}, "name": {}}

    :param total_shape: a pre-existing shape that we will merge fields into
    :param item: the current item being examined
    :returns: a shape for this item and its descendants (will be same dict as total_shape if that was passed in)
    """
    total_shape = total_shape or {}

    if isinstance(item, list):
        for x in item:
            total_shape = _get_shape_of_dicts(total_shape, x)
    elif isinstance(item, dict):
        for key, val in item.items():
            total_shape[key] = _get_shape_of_dicts(total_shape.get(key), val)

    return total_shape


def create_pyarrow_schema_for_resource(resource_type: str, batch_shape: dict) -> pyarrow.Schema:
    """
    Creates a PyArrow schema based off the named resource (like 'Observation').

    This schema will be as wide as the spec is and as deep as the batch_shape is.

    batch_shape is a dictionary tree of fields to include, like {"id": {}, "code": {"text": {}}}
    where empty dictionaries indicate no children (but the parent should still be included).
    """
    instance = fhirelementfactory.FHIRElementFactory.instantiate(resource_type, None)

    # fhirclient doesn't include `resourceType` in the list of properties. So do that manually.
    type_field = pyarrow.field("resourceType", pyarrow.string())

    return pyarrow.schema([type_field, *fhir_obj_to_pyarrow_fields(instance, batch_shape, level=0)])


def get_all_column_names(resource_type: str) -> list[str]:
    """
    Creates a list of all toplevel names for this resource
    """
    instance = fhirelementfactory.FHIRElementFactory.instantiate(resource_type, None)
    properties = map(FhirProperty._make, instance.elementProperties())
    return [prop.json_name for prop in properties]


def fhir_obj_to_pyarrow_fields(
    base_obj: fhirabstractbase.FHIRAbstractBase, batch_shape: dict, *, level: int
) -> list[pyarrow.Field]:
    """Convert a FHIR instance to a Pyspark StructType schema definition"""
    properties = map(FhirProperty._make, base_obj.elementProperties())
    return list(filter(None, map(partial(fhir_to_pyarrow_property, batch_shape=batch_shape, level=level), properties)))


def fhir_to_pyarrow_property(prop: FhirProperty, *, batch_shape: dict = None, level: int) -> pyarrow.Field | None:
    """Converts a single FhirProperty to a Pyspark StructField, returning None if this field should be skipped"""
    if batch_shape is not None:
        batch_shape = batch_shape.get(prop.json_name)
    if level > 1 and batch_shape is None:
        # If we're deep, only include fields we actually see in data
        return None

    pyarrow_type = fhir_to_pyarrow_type(prop.pytype, batch_shape, level=level)
    if pyarrow_type is None:
        return None

    # Wrap lists in an ListType
    if prop.is_list:
        pyarrow_type = pyarrow.list_(pyarrow_type)

    # Mark all types as nullable, don't worry about the prop.required field.
    # The ETL itself doesn't need to be in the business of validation, we just want to push the data through.
    return pyarrow.field(prop.json_name, pyarrow_type, nullable=True)


def fhir_to_pyarrow_type(pytype: type, batch_shape: dict, *, level: int) -> pyarrow.DataType | None:
    """Converts a basic python type to a Pyspark type, returning None if this element should be skipped"""
    if pytype is int:
        return pyarrow.int32()
    elif pytype is float:
        # TODO: the FHIR spec suggests that float64 might not even be enough:
        #  From https://www.hl7.org/fhir/R4/datatypes.html:
        #  "In object code, implementations that might meet this constraint are GMP implementations or equivalents
        #   to Java BigDecimal that implement arbitrary precision, or a combination of a (64 bit) floating point
        #   value with a precision field"
        #  But for now, we are matching the inferred types from before we used a pre-calculated schema.
        #  We can presumably up-scale this at some point if we find limitations.
        return pyarrow.float64()
    elif pytype is str:
        return pyarrow.string()
    elif pytype is bool:
        return pyarrow.bool_()
    elif pytype is fhirdate.FHIRDate:
        return pyarrow.string()  # just leave it as a string, like it appears in the JSON
    elif issubclass(pytype, fhirabstractbase.FHIRAbstractBase):
        # If this field is present in the inferred schema, that means some part of the data has this field.
        # So we should recurse and extend our normally shallow schema to be deep enough to include this field too.
        if level == 0 or batch_shape is not None:
            return pyarrow.struct(fhir_obj_to_pyarrow_fields(pytype(), batch_shape, level=level + 1))

        # Else skip this element entirely and do not descend, to avoid infinite recursion.
        # Note that in theory this might leave a struct with no child fields (if a struct's only children where also
        # structs), which parquet/spark would have an issue with -- it won't allow empty structs.
        # But in practice with FHIR, all BackboneElements have at least an id (string) field, so we dodge that bullet.
        return None
    raise ValueError(f"Unexpected type: {pytype}")