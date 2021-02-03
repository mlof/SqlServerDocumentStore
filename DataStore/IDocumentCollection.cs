using System.Threading.Tasks;
using Microsoft.SqlServer.Management.Smo;

namespace DataStore
{
    public interface IDocumentCollection<T>
    {
        public Task UpsertAsync(string id, string columnName, string columnValue, DataType? dataType = null);
        void AlterColumn(string name, DataType? dataType = null, string description = null);
    }
}