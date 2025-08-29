credentials += Credentials(
  "Sonatype Nexus Repository Manager",
  "central.sonatype.com",
  sys.env.getOrElse("SONATYPE_USERNAME",""),
  sys.env.getOrElse("SONATYPE_PASSWORD","")
)

