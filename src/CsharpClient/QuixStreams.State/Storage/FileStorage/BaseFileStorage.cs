using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace QuixStreams.State.Storage.FileStorage
{
    /// <summary>
    /// The directory storage containing the file storage for the single process access purposes
    /// The locking is implemented via the in-memory mutex
    /// </summary>
    public abstract class BaseFileStorage : IStateStorage
    {
        private readonly string storageDirectory;

        private static readonly string DefaultDir = Path.Combine(".", "state");
        private static readonly string FileNameSpecialCharacter = "~";

        /// <summary>
        /// Initializes a new instance of <see cref="BaseFileStorage"/>
        /// </summary>
        /// <param name="storageDirectory">Directory where to store the state</param>
        /// <param name="autoCreateDir">Create the directory if it doesn't exist</param>
        protected BaseFileStorage(string storageDirectory = null, bool autoCreateDir = true)
        {
            this.storageDirectory = storageDirectory ?? DefaultDir;

            if (autoCreateDir)
            {
                if (!Directory.Exists(this.storageDirectory))
                {
                    Directory.CreateDirectory(this.storageDirectory);
                }
            }
        }

        /// <summary>
        /// Lock type of the File Storage 
        /// </summary>
        protected enum LockType {

            /// <summary>
            /// Lock for read operations
            /// </summary>
            Reader,

            /// <summary>
            /// Lock for write operations
            /// </summary>
            Writer
        }
        
        /// <summary>
        /// Check if the key is valid for the constraints ( e.g. all keys must be lowercase )
        /// Throws exception if the key is not valid
        /// </summary>
        /// <param name="key">Storage key</param>
        /// <returns></returns>
        protected abstract void AssertKey(string key);

        /// <summary>
        /// Get file path from the storage key
        /// </summary>
        /// <param name="key">Storage key</param>
        /// <returns>File path</returns>
        private string GetFilePath(string key)
        {
            key = key.ToLower();
            return Path.Combine(storageDirectory, key);
        }

        /// <summary>
        /// Get storage key from the director file path
        /// </summary>
        /// <param name="path">Director file path</param>
        /// <returns>Storage element key</returns>
        private string GetKeyFromPath(string path)
        {
            return path.Substring(path.LastIndexOf(Path.DirectorySeparatorChar) + 1);
        }


        /// <summary>
        /// Save raw data into the key
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <param name="data">Raw byte[] representation of data</param>
        public async Task SaveRaw(string key, byte[] data)
        {
            key = key.ToLower();
            // lock on the directory
            using (await this.LockInternalKey("", LockType.Reader))
            {
                // lock on the key
                using (await this.LockInternalKey(key, LockType.Writer))
                {
                    using (FileStream sourceStream = File.Open(GetFilePath(key), FileMode.Create, FileAccess.Write, FileShare.None))
                    {
                        await sourceStream.WriteAsync(data, 0, data.Length);
                        await sourceStream.FlushAsync();
                        sourceStream.Close();
                        sourceStream.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Load raw data from the key
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <returns>Raw byte[] representation of data</returns>
        public async Task<byte[]> LoadRaw(string key)
        {
            key = key.ToLower();
            this.AssertKey(key);

            byte[] result;

            // lock on the directory
            using (await this.LockInternalKey("", LockType.Reader))
            {
                // lock writing
                using (await this.LockInternalKey(key, LockType.Reader))
                {
                    using (var sourceStream = File.Open(GetFilePath(key), FileMode.Open, FileAccess.Read, FileShare.Read))
                    {
                        result = new byte[sourceStream.Length];
                        await sourceStream.ReadAsync(result, 0, (int) sourceStream.Length);
                        sourceStream.Close();
                        sourceStream.Dispose();
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Remove key from the storage
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        /// <param name="key">Key of the element</param>
        public async Task RemoveAsync(string key)
        {
            key = key.ToLower();
            using ( await this.LockInternalKey("", LockType.Writer) )
            {
                var path = GetFilePath(key);
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
        }

        /// <summary>
        /// Check if storage contains key
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        /// <param name="key">Key of the element</param>
        /// <returns>Whether the storage contains the key</returns>
        public Task<bool> ContainsKeyAsync(string key)
        {
            key = key.ToLower();
            return Task.FromResult(
                File.Exists(GetFilePath(key))
            );
        }

        /// <summary>
        /// Recursively delete content of directory
        /// </summary>
        /// <param name="folderName">Directory path to remove</param>
        private void ClearFolder(string folderName)
        {
            DirectoryInfo dir = new DirectoryInfo(folderName);

            foreach (FileInfo fi in dir.GetFiles())
            {
                fi.Delete();
            }

            foreach (DirectoryInfo di in dir.GetDirectories())
            {
                ClearFolder(di.FullName);
                di.Delete();
            }
        }

        /// <summary>
        /// Clear the storage / remove all keys from the storage
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        public async Task ClearAsync()
        {
            using ( await this.LockInternalKey("", LockType.Writer) )
            {
                ClearFolder(this.storageDirectory);
            }
        }

        /// <inheritdoc/>
        public async Task<int> Count()
        {
            return (await GetAllKeysAsync()).Length;
        }

        /// <inheritdoc/>
        public bool IsCaseSensitive => false;
        
        /// <inheritdoc/>
        public IStateStorage GetOrCreateSubStorage(string subStorageName, string dbName = null)
        {
            return CreateNewStorageInstance(GetSubStoragePath(subStorageName));
        }

        private string GetSubStoragePath(string subStorageName)
        {
            var subPath = $"{this.storageDirectory}{Path.DirectorySeparatorChar}{subStorageName}";
            return subPath;
        }

        /// <summary>
        /// Creates a new storage instance using the path
        /// </summary>
        /// <param name="path">The path to create it at</param>
        /// <returns>New instance of the Storage</returns>
        protected abstract IStateStorage CreateNewStorageInstance(string path);
        
        /// <inheritdoc/>
        public bool DeleteSubStorage(string subStorageName, string dbName = null)
        {
            var subPath = GetSubStoragePath(subStorageName);
            if (!Directory.Exists(subPath)) return false;
            Directory.Delete(subPath, true);
            return true;
        }

        /// <inheritdoc/>
        public int DeleteSubStorages()
        {
            var deleted = 0;
            foreach (var subStorage in GetSubStorages())
            {
                Directory.Delete(GetSubStoragePath(subStorage), true);
                deleted++;
            }

            return deleted;
        }

        /// <inheritdoc/>
        public IEnumerable<string> GetSubStorages()
        {
            if (!Directory.Exists(this.storageDirectory)) return Array.Empty<string>();
            var dirNames = Directory.EnumerateDirectories(this.storageDirectory).Select(Path.GetFileName);
            return dirNames;
        }

        /// <summary>
        /// Get all keys in the storage ( used internally )
        /// This function is written in the asynchronous manner
        /// </summary>
        /// <returns>List of keys in the storage</returns>
        public async Task<string[]> GetAllKeysAsync()
        {
            //TODO: rewrite this function to the non-blocking Task fashion

            using(await this.LockInternalKey("", LockType.Reader))
            {
                string[] fileEntries = Directory.GetFiles(storageDirectory);
                List<string> result = new List<string>();
                foreach (string filePath in fileEntries)
                {
                    if (Path.GetFileName(filePath).Contains(FileNameSpecialCharacter))
                    {
                        //is internal ( special ) file
                        continue;
                    }

                    // Migrate from potentially different cases to lower case only
                    var key = GetKeyFromPath(filePath);
                    var lowerKey = key.ToLower();
                    if (key != lowerKey)
                    {
                        var lowerKeyPath = Path.Combine(storageDirectory, lowerKey);
                        if (File.Exists(lowerKeyPath)) // if the lowercase variant already exists
                        {
                            File.Move(filePath, $"{lowerKeyPath}_migrated");
                        }
                        else
                        {
                            File.Move(filePath, lowerKeyPath);
                        }
                    }
                    
                    
                    result.Add(lowerKey);
                }

                return result.ToArray();
            }
        }

        /// <summary>
        /// Perform internal lock on the single storage key
        /// This function is written in the asynchronous manner and returns Task
        /// </summary>
        /// <returns>IDisposable for disposing the lock</returns>
        protected abstract Task<IDisposable> LockInternalKey(string key, LockType type);

        /// <inheritdoc/>
        public void StartTransaction() => throw new NotSupportedException();
        
        /// <inheritdoc/>
        public bool CommitTransaction() => throw new NotSupportedException();
        
        /// <inheritdoc/>
        public bool CanPerformTransactions => false;
    }
}
