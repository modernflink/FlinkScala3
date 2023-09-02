lazy val mainRunner = project
  .in(file("mainRunner"))
  .dependsOn(RootProject(file(".")))
  .settings(
    // we set all provided dependencies to none, so that they are included in the classpath of mainRunner
    libraryDependencies := (RootProject(file(".")) / libraryDependencies).value
      .map { module =>
        module.configurations match {
          case Some("provided") =>
            module.withConfigurations(None)
          case _ => module
        }
      }
  )
