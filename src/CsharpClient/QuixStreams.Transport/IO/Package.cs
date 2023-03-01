using System;

namespace QuixStreams.Transport.IO
{
    /// <summary>
    /// Package holds the content value and its metadata with extra context relevant for transporting it.
    /// </summary>
    /// <typeparam name="TContent">The type of the content</typeparam>
    public sealed class Package<TContent> : Package
    {
        /// <summary>
        /// Initializes a new instance of <see cref="Package{TContent}"/>
        /// </summary>
        /// <param name="value">The lazy content value</param>
        /// <param name="metaData">The content meta data</param>
        /// <param name="transportContext">The extra context relevant for transporting the package</param>
        public Package(Lazy<TContent> value, MetaData metaData = null, TransportContext transportContext = null) :
            base(typeof(TContent), new Lazy<object>(() => value.Value), metaData, transportContext)
        {
            this.Value = value;
        }

        /// <summary>
        /// The content value of the package
        /// </summary>
        public new Lazy<TContent> Value { get; }
    }

    /// <summary>
    /// Package holds a value and its metadata with extra context relevant for transporting it.
    /// </summary>
    public class Package
    {
        /// <summary>
        /// Here for JSON deserialization
        /// </summary>
        protected Package()
        {
        }

        /// <summary>
        /// Initializes a new instance of <see cref="Package"/>
        /// </summary>
        /// <param name="type">The type of the content value</param>
        /// <param name="value">The lazy content value</param>
        /// <param name="metaData">The content meta data</param>
        /// <param name="transportContext">The extra context relevant for transporting the package</param>
        public Package(Type type, Lazy<object> value, MetaData metaData = null, TransportContext transportContext = null)
        {
            this.Type = type;
            this.Value = value;
            this.TransportContext = transportContext ?? new TransportContext();
            this.MetaData = metaData ?? MetaData.Empty;
        }

        /// <summary>
        /// The content value of the package
        /// </summary>
        public Lazy<object> Value { get; protected set; }

        /// <summary>
        /// The transport context of the package
        /// </summary>
        public TransportContext TransportContext { get; }

        /// <summary>
        /// The content meta data
        /// </summary>
        public MetaData MetaData { get; protected set; }

        /// <summary>
        /// The type of the value
        /// </summary>
        public Type Type { get; protected set; }

        /// <summary>
        /// Converts the Package to type provided if possible
        /// </summary>
        /// <typeparam name="TContent">The type of the content</typeparam>
        /// <returns>Null if failed to convert else the typed QuixStreams.Transport.Fw.Package</returns>
        public virtual bool TryConvertTo<TContent>(out Package<TContent> convertedPackage)
        {
            if (this is Package<TContent> pack)
            {
                convertedPackage = pack;
                return true;
            }

            if (!typeof(TContent).IsAssignableFrom(this.Type))
            {
                convertedPackage = null;
                return false;
            }

            convertedPackage = new Package<TContent>(new Lazy<TContent>(() => (TContent) this.Value.Value), this.MetaData, this.TransportContext);
            return true;
        }
    }
}