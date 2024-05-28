import os
import sys
from dagster import asset


@asset(required_resource_keys={"qnap_film"})
def make_assessment(context):
    '''
    software defined asset example
    run assessment script to chose
    splitting/encoding path
    '''
    pass


@asset
def trigger_splitting(context):
    '''
    sda splitting where needed
    '''
    pass

