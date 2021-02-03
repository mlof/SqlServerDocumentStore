using System;
using System.Threading.Tasks;
using Dapper;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Smo;

namespace DataStore
{
    public class DocumentCollection<T> : IDocumentCollection<T>
    {
        private readonly SqlConnection _sqlConnection;

        public DocumentCollection(SqlConnection sqlConnection, Table table)
        {
            _sqlConnection = sqlConnection;
            this.Table = table;
        }

        public Table Table { get; set; }

        /// <inheritdoc />
        public async Task UpsertAsync(string id, string columnName, string columnValue, DataType? dataType)
        {
            if (!Table.Columns.Contains(columnName) && dataType != null)
            {
                AlterColumn(columnName, dataType);
            }

            var count = await _sqlConnection.ExecuteScalarAsync<int>(
                "SELECT COUNT(*) FROM Documents WHERE Id = @Id AND Type = @Type",
                new {Id = id, Type = typeof(T).Name});
            if (count == 0)
            {
                await _sqlConnection.ExecuteAsync(
                    "INSERT INTO Documents (Id, Type) VALUES (@Id, @Type)",
                    new {Id = id, Type = typeof(T).Name, ColumnName = columnName});
            }


            if (Table.Columns.Contains(columnName))
            {
                await _sqlConnection.ExecuteAsync(
                    $"UPDATE Documents SET [{columnName}]= @ColumnValue WHERE Id = @Id AND Type = @Type",
                    new {Id = id, Type = typeof(T).Name, ColumnName = columnName, ColumnValue = columnValue});
            }
        }

        /// <inheritdoc />
        public void AlterColumn(string name, DataType dataType, string? description = null)
        {
            Column column;
            if (!Table.Columns.Contains(name))
            {
                if (dataType == null)
                {
                    throw new ArgumentException(nameof(dataType));
                }

                else
                {
                    column = new Column(Table, name) {DataType = dataType, IsSparse = true};

                    Table.Columns.Add(column);
                    Table.Alter();
                }
            }
            else
            {
                column = Table.Columns[name];
            }

            if (dataType != null || description != null)
            {
                if (description != null)
                {
                    ExtendedProperty descriptionProperty;
                    if (column.ExtendedProperties.Contains("MS_Description"))
                    {
                        descriptionProperty = column.ExtendedProperties["MS_Description"];
                        descriptionProperty.Value = description;
                    }
                    else
                    {
                        descriptionProperty = new ExtendedProperty(column, "MS_Description", description);
                        column.ExtendedProperties.Add(descriptionProperty);
                    }

                    descriptionProperty.CreateOrAlter();
                }

                column.Alter();
            }
        }
    }
}