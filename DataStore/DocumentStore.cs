using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;

namespace DataStore
{
    public class DocumentStore : IDisposable
    {
        private SqlConnection connection;
        private ServerConnection serverConnection;
        private Server server;
        private Database database;
        private Table table;

        public DocumentStore(SqlConnection connection)
        {
            this.connection = connection;

            this.serverConnection = new ServerConnection(this.connection);
            this.server = new Server(serverConnection);
            this.database = server.Databases["DocumentStore"];


            if (database.Tables.Contains("Documents"))
            {
                this.table = database.Tables["Documents"];
            }
            else
            {
                this.table = new Table(database, "Documents", "dbo");

                var idColumn = new Column(table, "Id") {DataType = DataType.VarChar(250)};
                this.table.Columns.Add(idColumn);

                var typeColumn = new Column(table, "Type") {DataType = DataType.VarChar(250)};
                this.table.Columns.Add(typeColumn);
            }
        }

        public IDocumentCollection<T> GetCollection<T>()
        {

            return new DocumentCollection<T>(connection, table);

        }
        
   
        public async Task Initialize()
        {
            if (!database.Tables.Contains("Documents"))
            {
                table.Create();
            }
        }

        /// <inheritdoc />
        public void Dispose()
        {
            connection?.Dispose();
        }
    }
}