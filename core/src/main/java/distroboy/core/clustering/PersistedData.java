package distroboy.core.clustering;

import distroboy.schemas.Value;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

public interface PersistedData {
  ConcurrentMap<UUID, Supplier<Iterator<Value>>> storedReferences = new ConcurrentHashMap<>();
}
