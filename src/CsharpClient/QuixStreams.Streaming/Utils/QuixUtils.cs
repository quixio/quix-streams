using System.Text.RegularExpressions;

namespace QuixStreams.Streaming.Utils
{
    /// <summary>
    /// Quix extension methods used across the library
    /// </summary>
    public static class QuixUtils
    {
        /// <summary>
        /// Extract WorkspaceId from TopicId information
        /// </summary>
        /// <param name="topicId">Topic Id</param>
        /// <param name="workspaceId">Workspace Id (output)</param>
        /// <returns>Whether the function could extract the Workspace Id information</returns>
        public static bool TryGetWorkspaceIdPrefix(string topicId, out string workspaceId)
        {
            // Regex explained:
            // starts with several something that isn't a -, like myorg123
            // then it can continue with - and a hash, like -a11ef which is min 1 or 8 long
            // same rule for workspace
            // then it has something at the end
            // resulting in a minimum of my myorg123-myws-mytopicname
            // and a maximum of myorg123-11cdefff-myws-01234567-doesntmatterwhateverelse-can-be-here-as-topic-name
            var workspaceIdRegex = Regex.Match(topicId, "^([^-]+(\\-[0-9ABCDEFabcdef]{1,8})?\\-[^-]+(\\-[0-9ABCDEFabcdef]{1,8})?\\-)[^-]");
            if (workspaceIdRegex.Success)
            {
                workspaceId = workspaceIdRegex.Groups[1].Value;
                return true;
            }

            workspaceId = null;
            return false;
        }
    }
}