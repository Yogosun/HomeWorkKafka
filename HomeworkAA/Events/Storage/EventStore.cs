using System.Collections.Generic;

namespace HomeworkAA.Events.Storage
{
    public class EventStore
    {
        private readonly List<object> _events = new List<object>();

        public void SaveEvent(object eventItem)
        {
            _events.Add(eventItem);
        }

        public IReadOnlyList<object> GetEvents()
        {
            return _events.AsReadOnly();
        }
    }
}
