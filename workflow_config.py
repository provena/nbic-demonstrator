from pydantic import BaseModel
import json
import registry
from mdsisclienttools.auth.TokenManager import BearerAuth
from typing import Callable


GetAuthFunction = Callable[[], BearerAuth]


class ModelOutputs(BaseModel):
    # This is a key part of the model run configuration - it specifies a set of
    # expected dataset references - the value here should be the handle
    # identifier of the Provena registered Dataset

    hourly_ffdi: str
    hourly_ffdi_template: str

    def validate_entities(self, registry_endpoint: str, auth: GetAuthFunction) -> bool:
        print("Validating registered output datasets...")

        datasets = [
            self.hourly_ffdi
        ]

        templates = [
            self.hourly_ffdi_template
        ]

        for id in datasets:
            try:
                registry.fetch_dataset(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Dataset: {id=}. Exception: {e}.")
                return False

        for id in templates:
            try:
                registry.fetch_dataset_template(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Dataset Template: {id=}. Exception: {e}.")
                return False

        return True


class ModelInputs(BaseModel):
    # This is a key part of the model run configuration - it specifies a set of
    # expected dataset references - the value here should be the handle
    # identifier of the Provena registered Dataset

    # each part includes the template ID and the dataset ID which matches it

    # The hourly temperature dataset - i.e.
    # nbic.catalog_s3_stage1.weather.projected.to_path('AU_hourly_temperature_C.zarr')
    hourly_temperature: str
    hourly_temperature_template: str

    # The relative humidity dataset - i.e.
    # nbic.catalog_s3_stage1.weather.projected.to_path('AU_hourly_relative_humidity_percent.zarr')
    relative_humidity: str
    relative_humidity_template: str

    # The wind speed dataset - i.e.
    # nbic.catalog_s3_stage1.weather.baseline.to_path('AU_hourly_wind_speed_mps_updated_220224_corrected.zarr')
    wind_speed: str
    wind_speed_template: str

    # The 'daily McADF' - not sure what this is - i.e.
    # nbic.catalog_s3_stage1.weather.projected.to_path('AU_daily_McADF_improved_uncapped.zarr')
    daily_mc_adf: str
    daily_mc_adf_template: str

    def validate_entities(self, registry_endpoint: str, auth: GetAuthFunction) -> bool:
        print("Validating registered input datasets...")

        datasets = [
            self.hourly_temperature,
            self.relative_humidity,
            self.wind_speed,
            self.daily_mc_adf
        ]

        templates = [
            self.hourly_temperature_template,
            self.relative_humidity_template,
            self.wind_speed_template,
            self.daily_mc_adf_template
        ]

        for id in datasets:
            try:
                registry.fetch_dataset(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Dataset: {id=}. Exception: {e}.")
                return False

        for id in templates:
            try:
                registry.fetch_dataset_template(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Dataset Template: {id=}. Exception: {e}.")
                return False

        return True


class ModelAssociations(BaseModel):
    # This links the model run to a user/organisation

    # registered Person
    person: str

    # registered organisation
    organisation: str

    def validate_entities(self, registry_endpoint: str, auth: GetAuthFunction) -> bool:
        print("Validating registered associations...")

        people = [
            self.person
        ]

        organisations = [
            self.organisation
        ]

        for id in people:
            try:
                registry.fetch_person(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Person: {id=}. Exception: {e}.")
                return False

        for id in organisations:
            try:
                registry.fetch_organisation(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Organisation: {id=}. Exception: {e}.")
                return False

        return True


class ModelConfigurationEntities(BaseModel):
    # The registered model run workflow template
    workflow_template: str

    def validate_entities(self, registry_endpoint: str, auth: GetAuthFunction) -> bool:
        print("Validating registered associations...")

        wf_templates = [
            self.workflow_template
        ]

        for id in wf_templates:
            try:
                registry.fetch_model_run_workflow_template(
                    registry_endpoint=registry_endpoint, id=id, auth=auth())
            except Exception as e:
                print(
                    f"Encountered exception while validating Model Run Workflow Template: {id=}. Exception: {e}.")
                return False

        return True


class HourlyFFDIWorkflowConfig(BaseModel):
    inputs: ModelInputs
    outputs: ModelOutputs
    associations: ModelAssociations
    workflow_configuration: ModelConfigurationEntities

    def pprint(self) -> None:
        print(json.dumps(json.loads(self.json()), indent=2))

    @staticmethod
    def dump_example(path: str) -> None:
        empty_example = HourlyFFDIWorkflowConfig(
            inputs=ModelInputs(
                hourly_temperature="TODO",
                relative_humidity="TODO",
                daily_mc_adf="TODO",
                wind_speed="TODO",
                hourly_temperature_template="TODO",
                relative_humidity_template="TODO",
                daily_mc_adf_template="TODO",
                wind_speed_template="TODO"
            ),
            outputs=ModelOutputs(
                hourly_ffdi="TODO",
                hourly_ffdi_template="TODO"
            ),
            associations=ModelAssociations(
                person="TODO",
                organisation="TODO"
            ),
            workflow_configuration=ModelConfigurationEntities(
                workflow_template="TODO"
            )
        )

        json_content = json.dumps(json.loads(empty_example.json()), indent=2)
        with open(path, 'w') as f:
            f.write(json_content)

    def validate_entities(self, registry_endpoint: str, auth: GetAuthFunction) -> bool:
        print("Validating registered Provena entities in config")

        inputs = self.inputs.validate_entities(
            registry_endpoint=registry_endpoint, auth=auth)

        if not inputs:
            print("Failed inputs validation.")
            return False

        outputs = self.outputs.validate_entities(
            registry_endpoint=registry_endpoint, auth=auth)

        if not outputs:
            print("Failed outputs validation.")
            return False

        associations = self.associations.validate_entities(
            registry_endpoint=registry_endpoint, auth=auth)

        if not associations:
            print("Failed associations validation.")
            return False

        model_config = self.workflow_configuration.validate_entities(
            registry_endpoint=registry_endpoint, auth=auth)

        if not model_config:
            print("Failed workflow configuration validation.")
            return False

        print("Validation successful...")
        return True


def load_config(path: str) -> HourlyFFDIWorkflowConfig:
    """

    Loads the config from the specific path. 

    This should be a JSON file which satisfies the model above.

    Args:
        path (str): The file path.

    Raises:
        Exception: Failed loading file/parsing

    Returns:
        HourlyFFDIWorkflowConfig: The parsed config object
    """

    try:
        return HourlyFFDIWorkflowConfig.parse_file(path=path)
    except Exception as e:
        raise Exception(
            f"An exception occurred when loading config from {path = }. Exception: {e}.")


if __name__ == "__main__":
    HourlyFFDIWorkflowConfig.dump_example(path="configs/template.json")
