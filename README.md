# gpkg2dxf - Generates DXF files from GPKG files

This is a standalone application of [gretl's](https://github.com/sogis/gretl) [Gpkg2DxfStep](https://github.com/sogis/gretl/blob/master/gretl/src/main/java/ch/so/agi/gretl/steps/Gpkg2DxfStep.java)

## Running Gpkg2Dxf

Gpkg2Dxf expects two arguments:

| Arguments        | Description                     |
| -------------    |-------------                    |
| gpkgFile         | Input GPKG-File                 |
| outputDir        | Output directory, of the result |
| log (Optional)   | Log file path                   |

and it can be started with

    java -jar Gpkg2Dxf.jar --gpkgFile file.gpkg --outputDir myDir/

## Development
### Build from Source

    gradle build
### Run Gpkg2Dxf with gradle

    gradle run --args="--gpkgFile file.gpkg --outputDir myDir/"