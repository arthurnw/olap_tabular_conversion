#region Load Assemblies
Add-Type -AssemblyName 'Microsoft.AnalysisServices, Version=14.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91'
Add-Type -AssemblyName 'Microsoft.AnalysisServices.Core, Version=14.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91'
Add-Type -AssemblyName 'Microsoft.AnalysisServices.Tabular, Version=14.0.0.0, Culture=neutral, PublicKeyToken=89845dcd8080cc91'
#endregion

#region Load Assemblies (Deprecated Method)
# Use these methods if there are problems with differing assembly versions
<#
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices")
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices.Core")
[System.Reflection.Assembly]::LoadWithPartialName("Microsoft.AnalysisServices.Tabular")
#>
#endregion

#region Globals
$olapServerString = 'localhost\olap'
$olapDbName = 'WideWorldImportersMultidimensionalCube'
$cubeName = 'Wide World Importers'

$serverString = 'localhost'
$dbName = 'WWI'

    #region OLAP to Tabular Data Type Map
    $dataTypeMap = @{}
    $dataTypeMap["BigInt"] = "Int64"
    $dataTypeMap["Binary"] = "Binary"
    $dataTypeMap["Boolean"] = "Boolean"
    $dataTypeMap["Char"] = "String"
    $dataTypeMap["Currency"] = "Decimal"
    $dataTypeMap["Date"] = "DateTime"
    $dataTypeMap["DateTime"] = "DateTime"
    $dataTypeMap["Decimal"] = "Decimal"
    $dataTypeMap["Double"] = "Double"
    $dataTypeMap["Integer"] = "Int64"
    $dataTypeMap["Int16"] = "Int64"
    $dataTypeMap["Int32"] = "Int64"
    $dataTypeMap["Int64"] = "Int64"
    $dataTypeMap["Single"] = "Double"
    $dataTypeMap["SmallInt"] = "Int64"
    $dataTypeMap["String"] = "String"
    $dataTypeMap["TinyInt"] = "Int64"
    $dataTypeMap["UnsignedBigInt"] = "Int64"
    $dataTypeMap["UnsignedInt"] = "Int64"
    $dataTypeMap["UnsignedSmallInt"] = "Int64"
    $dataTypeMap["UnsignedTinyInt"] = "Int64"
    $dataTypeMap["UInt16"] = "Int64"
    $dataTypeMap["UInt32"] = "Int64"
    $dataTypeMap["UInt64"] = "Int64"
    $dataTypeMap["Variant"] = "Variant"
    $dataTypeMap["WChar"] = "String"
    #endregion
#endregion

#region Connection/Instance
    #region OLAP Server, DB, and Cube
    $olapServer = New-Object Microsoft.AnalysisServices.Server
    $olapServer.Connect($olapServerString)

    $olapDb = $olapServer.Databases.GetByName($olapDbName)
    $cube = $olapDb.Cubes.GetByName($cubeName)
    $sourceTables = $cube.DataSourceView.Schema.Tables        
    #endregion    

    #endregion
    
    #region Server
    $server = New-Object Microsoft.AnalysisServices.Tabular.Server
    $server.Connect($serverString)
    #$server.Connect('asazure://northcentralus.asazure.windows.net/tlvntanwsqlas')
    #endregion

    #region Database
    $db = New-Object Microsoft.AnalysisServices.Tabular.Database -Property @{
        Name = $server.Databases.GetNewName($dbName);
        ID = $server.Databases.GetNewID($dbName);
        CompatibilityLevel = 1400;
        StorageEngineUsed = [Microsoft.AnalysisServices.StorageEngineUsed]::TabularMetadata # Relates to TMSL (JSON) vs. XMLA (XML)
    }
    #endregion
#endregion

#region Model
$model = New-Object Microsoft.AnalysisServices.Tabular.Model -Property @{
    Name = 'EDW'
}

    #region Data Sources
    <#
    $providerDataSource = New-Object Microsoft.AnalysisServices.Tabular.ProviderDataSource -Property @{
        Name = 'WideWorldImportersDW_OLEDB';
        ConnectionString = 'Provider=SQLNCLI11;Data Source=localhost;Initial Catalog=WideWorldImportersDW;Integrated Security=SSPI;Persist Security Info=false';
        ImpersonationMode = [Microsoft.AnalysisServices.Tabular.ImpersonationMode]::ImpersonateServiceAccount
    }
    #>
    $structDataSource = New-Object Microsoft.AnalysisServices.Tabular.StructuredDataSource -Property @{
        Name = 'WideWorldImportersDW';
        ConnectionDetails = '{"protocol":"tds","address":{"server":"localhost","database":"WideWorldImportersDW"},"authentication":null,"query":null}';
        Credential = '{"AuthenticationKind": "ServiceAccount","EncryptConnection": false}'
    }
    #$model.DataSources.Add($providerDataSource)
    $model.DataSources.Add($structDataSource)
    #endregion

    #region Tables - Dimensions
    # Iterate through each OLAP dimension
    foreach ($dim in $cube.Dimensions) {
    Write-Host "Generating dimension" $dim.Name
        #region Table
        # Generate the table from the dimension's source table, acquired via joining the DSV to the key attribute's source column
        $dimTableID = $dim.Dimension.KeyAttribute.KeyColumns[0].Source.TableID
        $sourceTable = $sourceTables | Where-Object {$_.TableName -eq $dimTableID}
        $sourceSchemaName = $sourceTable.ExtendedProperties.DbSchemaName
        $sourceTableName = $sourceTable.ExtendedProperties.DbTableName
        $sourceColumns = $cube.DataSourceView.Schema.Tables[$dimTableID].Columns
        $table = New-Object Microsoft.AnalysisServices.Tabular.Table -Property @{Name = $dim.Name}
            
            #region Base Partition
            $partition = New-Object Microsoft.AnalysisServices.Tabular.Partition -Property @{
                Name = "Partition" #$dim.Name;
                DataView = [Microsoft.AnalysisServices.Tabular.DataViewType]::Full;
                Source = New-Object Microsoft.AnalysisServices.Tabular.MPartitionSource -Property @{
                    Expression = "let`n    Source = #`"WideWorldImportersDW`",`n    Table = Source{[Schema=`"$sourceSchemaName`",Item=`"$sourceTableName`"]}[Data]`nin`n    Table"
                }
            }
        
            $table.Partitions.Add($partition)
            #endregion

            #region Columns
            # Iterate through dimension attributes and generate column objects
            
            foreach ($att in $dim.Attributes) {
                $sourceColumn = $sourceColumns | Where-Object {$_.ColumnName -eq $att.Attribute.Name}
                # Skip if it's a logical (i.e. calculated) column
                # To do: add support for this, though it's not best practice
                $isLogical = if ($sourceColumn.ExtendedProperties.IsLogical -eq $null) { $false } else { $true }
                
                # Set properties specific to the key column
                if ($att.Attribute.Usage -eq "Key") {
                    $isHidden = $true
                    $isKey = $true
                    $colName = $att.Attribute.KeyColumns[0].Source.ColumnID
                # All non-key columns should be visible
                } else {
                    $isHidden = $false
                    $isKey = $false
                    $colName = $att.Attribute.Name
                }

                $dataTypeString = $dataTypeMap[[Convert]::ToString($att.Attribute.KeyColumns[0].DataType)]
                $dataTypeEnum = [Microsoft.AnalysisServices.Tabular.DataType] $dataTypeString

                if ($isLogical) {                    
                    $column = New-Object Microsoft.AnalysisServices.Tabular.CalculatedColumn -Property @{
                        Name = $att.Attribute.Name;
                        Expression = "`"FILL IN DAX EXPRESSION HERE`""; # This needs to be translated manually
                        IsHidden = $isHidden;
                        #IsKey = $isKey # Can't use calculated columns as keys
                        DataType = $dataTypeEnum;
                        SummarizeBy = [Microsoft.AnalysisServices.Tabular.AggregateFunction]::None
                    }
                } else {
                    $column = New-Object Microsoft.AnalysisServices.Tabular.DataColumn -Property @{
                        Name = $colName;
                        SourceColumn = $sourceColumn.ExtendedProperties.DbColumnName #$att.Attribute.KeyColumns[0].Parent.Name;
                        IsHidden = $isHidden;
                        IsKey = $isKey;
                        DataType = $dataTypeEnum;
                        SummarizeBy = [Microsoft.AnalysisServices.Tabular.AggregateFunction]::None
                    }
                }
                Write-Host "`tAdding dimension column" $column.Name
                $table.Columns.Add($column)
            }
            #endregion
        #endregion

        #region Hierarchies
        foreach ($h in $dim.Hierarchies) {            
            $hierarchy = New-Object Microsoft.AnalysisServices.Tabular.Hierarchy -Property @{Name = $h.Hierarchy.Name}
            foreach ($l in $h.Hierarchy.Levels) {
                $level = New-Object Microsoft.AnalysisServices.Tabular.Level -Property @{
                    Name = $l.Name;
                    Column = $table.Columns | Where-Object {$_.Name -eq $l.SourceAttribute.Name}
                    Ordinal = $h.Hierarchy.Levels.IndexOf($l.Name)
                }

                $hierarchy.Levels.Add($level)
            }
            Write-Host "`tAdding dimension hierarchy" $hierarchy.Name
            $table.Hierarchies.Add($hierarchy)
        }
        #endregion

        Write-Host "Saving dimension" $table.Name
        $model.Tables.Add($table)
    }
    #endregion

    #region Tables - Measure Groups
    # Iterate through each OLAP measure group
    foreach ($mg in $cube.MeasureGroups) {
        Write-Host "Generating fact" $mg.Name
        #region Table
        $factTableID = $mg.Partitions[0].Source.TableID
        $sourceTable = $sourceTables | Where-Object {$_.TableName -eq $factTableID}
        $sourceSchemaName = $sourceTable.ExtendedProperties.DbSchemaName
        $sourceTableName = $sourceTable.ExtendedProperties.DbTableName
        $sourceColumns = $cube.DataSourceView.Schema.Tables[$factTableID].Columns
        $table = New-Object Microsoft.AnalysisServices.Tabular.Table -Property @{Name = $mg.Name}
        # Generate table from source binding
            #region Base Partition
            $partition = New-Object Microsoft.AnalysisServices.Tabular.Partition -Property @{
                Name = "Partition" #$dim.Name;
                DataView = [Microsoft.AnalysisServices.Tabular.DataViewType]::Full;
                Source = New-Object Microsoft.AnalysisServices.Tabular.MPartitionSource -Property @{
                    Expression = "let`n    Source = #`"WideWorldImportersDW`",`n    Table = Source{[Schema=`"$sourceSchemaName`",Item=`"$sourceTableName`"]}[Data]`nin`n    Table"
                }
            }
        
            $table.Partitions.Add($partition)
            #endregion

            #region Columns
            # Iterate through fact columns and generate column objects
            foreach ($col in $sourceColumns) {
                #$sourceColumn = $sourceColumns | Where-Object {$_.ColumnName -eq $measure.Source.Source.ColumnID}
                               
                $dataTypeString = $dataTypeMap[[Convert]::ToString($col.DataType).Replace("System.","")]
                $dataTypeEnum = [Microsoft.AnalysisServices.Tabular.DataType] $dataTypeString
                #$formatString = $measure.FormatString

                $column = New-Object Microsoft.AnalysisServices.Tabular.DataColumn -Property @{
                    Name = $col.ExtendedProperties.DbColumnName;
                    SourceColumn = $col.ExtendedProperties.DbColumnName;
                    #FormatString = $formatString;
                    IsHidden = $true;
                    DataType = $dataTypeEnum
                }
                #region Base Measures
                <#
                $baseMeasure = New-Object Microsoft.AnalysisServices.Tabular.Measure -Property @{
                    Name = "Total $measure";
                    Expression = "SUM('$table'[$measure])";
                    FormatString = $formatString
                }
                #>
                #endregion
                Write-Host "`tAdding fact column" $column.Name
                $table.Columns.Add($column)
            }            
            #endregion
        #endregion
        
        Write-Host "Saving fact" $table.Name
        $model.Tables.Add($table)
        #region Relationships - Dimension Usage
        # Iterate through OLAP bus matrix (dimension usage) to create relationships
        foreach ($dim in $mg.Dimensions) {
            # Skip this dimension if it's not a regular dimension mapping
            if ($dim.GetType() -ne [Microsoft.AnalysisServices.RegularMeasureGroupDimension]) { continue }

            $mgDimAttributes = $dim.Attributes
            # Filter down to the granularity attribute - i.e. the key
            $granularity = $mgDimAttributes | Where-Object {$_.Type -eq [Microsoft.AnalysisServices.MeasureGroupAttributeType]::Granularity}

            # Find Tabular table matching the cube dimension by name
            $toTable = $model.Tables | Where-Object {$_.Name -eq $dim.CubeDimension.Name}
            # Find Tabular column matching the granularity attribute by name
            $toCol = $toTable.Columns | Where-Object {$_.Name -eq $granularity.Attribute.Name}
            # Find Tabular table matching the measure group by name
            $fromTable = $model.Tables | Where-Object {$_.Name -eq $mg.Name}
            # Find Tabular column matching the fact table key by name
            $fromCol = $fromTable.Columns | Where-Object {$_.Name -eq $granularity.KeyColumns[0].Source.ColumnID}

            Write-Host "Mapping" $dim.CubeDimensionID "via" $fromTable.Name "[" $fromCol.Name "] to" $toTable.Name "[" $toCol.Name "]"
            
            $rel = New-Object Microsoft.AnalysisServices.Tabular.SingleColumnRelationship -Property @{
                Name = "{0} [{1}] - {2} [{3}]" -f ($fromTable.Name, $fromCol.Name, $toTable.Name, $toCol.Name);
                ToColumn = $toCol;
                FromColumn = $fromCol;
                ToCardinality = [Microsoft.AnalysisServices.Tabular.RelationshipEndCardinality]::One;
                FromCardinality = [Microsoft.AnalysisServices.Tabular.RelationshipEndCardinality]::Many
            }

            $model.Relationships.Add($rel)
        }
        #endregion
    }
    #endregion

    #region Roles
    # Iterate through roles and just create them - need to manually define role formula
    foreach ($olapRole in $olapDb.Roles) {
        $role = New-Object Microsoft.AnalysisServices.Tabular.ModelRole -Property @{
            Name = $olapRole.Name
        }

        Write-Host "Adding role" $role.Name
        $model.Roles.Add($role)
    }
    #endregion

$db.Model = $model # Attach the model to the database
#endregion

#region Update Server Contents
$server.Databases.Add($db)
$db.Update([Microsoft.AnalysisServices.UpdateOptions]::ExpandFull)
#endregion

#region Refresh

#endregion

#region Close
$server.Disconnect()
#endregion