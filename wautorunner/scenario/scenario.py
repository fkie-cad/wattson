from pathlib import Path
import shutil
import yaml

class ScenarioBuilder:
    
    @staticmethod
    def build(originPath: Path, targetPath: Path):
        """ Logic to build the scenario """
        # Copy the scenario from originPath to targetPath
        if originPath.exists():
            shutil.copytree(originPath, targetPath, dirs_exist_ok=True)
        else:
            raise FileNotFoundError(f"Origin path {originPath} does not exist.")

        # Create a Scenario object
        return Scenario(targetPath)


class Scenario:
    def __init__(self, scenarioPath: Path):
        self.scenarioPath: Path = scenarioPath
        self.powerGridFilePath: Path = self.scenarioPath.joinpath("power-grid.yml")

    def getPowerGridModel(self) -> dict:
        # Logic to get the power grid file
        with open(self.powerGridFilePath, 'r') as file:
            return yaml.load(file, Loader=yaml.Loader)
        
    def savePowerGridModel(self, powerGridModel: dict):
        # Logic to save the power grid file
        with open(self.powerGridFilePath, 'w') as file:
            file.truncate(0)
            yaml.dump(powerGridModel, file)

    def getScenarioPath(self) -> Path:
        return self.scenarioPath

    def run(self):
        # Logic to run the scenario
        pass