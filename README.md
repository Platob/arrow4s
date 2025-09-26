
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
```scala
import io.github.platob.arrow4s.core.ArrowArray
import io.github.platob.arrow4s.core.cast.NumericOpsPlus._

// Raw build
val values = Seq(1, 2, 3, 4, 5)
val array = ArrowArray(values:_*)

array.toSeq == values
array.as[Option[Int]].toSeq == values.map(Option.apply)
```
