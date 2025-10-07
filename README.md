
# arrow4s starter

Minimal multi-module SBT template for Scala **2.12** / **2.13** with:
- `core` module for platform-agnostic code
- `spark` module for Apache Spark (sql/core), `provided` scope
- Publish-ready to Maven Central via **sbt-ci-release**
- GitHub Actions workflows (test + release)
- IntelliJ-friendly (import as SBT project)
- Scalafmt + recommended compiler flags

## Quickstart

```bash
sbt +test
sbt +publishLocal
```

## Import into IntelliJ
- Open IntelliJ IDEA → **New / Open** → select this folder.
- Ensure Scala plugin is installed; use **sbt shell** for builds.

## Releasing (Maven Central via Sonatype)
This project uses **sbt-ci-release**. Configure the following **GitHub Secrets** on your repo:
- `PGP_PASSPHRASE` (optional, if your key is passphrase-protected)
- `PGP_SECRET` (ASCII-armored private key)
- `SONATYPE_USERNAME`
- `SONATYPE_PASSWORD`

On pushing a **tag** like `v0.1.0`, the CI pipeline will publish:
- `*-SNAPSHOT` to snapshots on pushes to default branch
- tagged releases to Sonatype (then Central)

## Coordinates
Group is pre-set to `io.github.platob`. Update `organization` if needed.

## Example usage

### In memory

### Primitive arrays
```scala
import io.github.platob.arrow4s.core.ArrowArray

// Raw build
val values: Seq[Int] = Seq(1, 2, 3, 4, 5)
val array = ArrowArray(values:_*)

array == values
array.as[Option[Double]] == values.map(v => Option(v.toDouble))
```

### Nested arrays
```scala
import io.github.platob.arrow4s.core.ArrowArray

case class TestRecord(a: Int, b: String, c: Option[Double])

val records = (1 to 5).map(i => TestRecord(i, s"str_$i", if (i % 2 == 0) Some(i.toDouble) else None))
val array = ArrowArray(records:_*)
val tuples = array.as[(Int, String, Option[Double])]

array == records
```

### IPC
```scala
import io.github.platob.arrow4s.io.ipc.IPCInput
import io.github.platob.arrow4s.core.arrays.nested.ArrowRecord

val ipc = IPCInput.file(filePath = path)

val batches = ipc.batches(closeAtEnd = false).toList
val batch = batches.head
val record: ArrowRecord = batch(0)
```