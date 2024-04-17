<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

  <xsl:output method="xml" encoding="UTF-8"/>
  <xsl:template match="/CountryList">
    <xsl:text>INSERT INTO country0 (code, code3, name) VALUES</xsl:text>
    <xsl:apply-templates select="country"/>
    <xsl:text>;</xsl:text>
    <xsl:text>&#10;</xsl:text>
    <xsl:text></xsl:text>
    <xsl:apply-templates select="country/city"/>
    <xsl:text>;</xsl:text>
  </xsl:template>

  <xsl:template match="country">
    <xsl:text>&#10;('</xsl:text>
    <xsl:value-of select="@sc"/>
    <xsl:text>', '</xsl:text>
    <xsl:value-of select="@lc"/>
    <xsl:text>', '</xsl:text>
    <xsl:value-of select="@co_name"/>
    <xsl:text>')</xsl:text>
    <xsl:if test="position() != last()">
      <xsl:text>,</xsl:text>
    </xsl:if>
  </xsl:template>

  <xsl:template match="city">
    <xsl:text>&#10;INSERT INTO city0 (city_id, city_name, latitude, longitude, postal_code, country_code) VALUES(CAST(
        CONVERT(VARBINARY(MAX), '0x' + CONVERT(VARCHAR(MAX), CAST(N'' AS XML).value('xs:base64Binary("</xsl:text>
    <xsl:value-of select="@city_id"/>
    <xsl:text>")', 'VARBINARY(MAX)'), 2), 1) AS BINARY(16)), N'</xsl:text>
    <xsl:value-of select="@ci_name"/>
    <xsl:text>', '</xsl:text>
    <xsl:value-of select="geo/lat"/>
    <xsl:text>', '</xsl:text>
    <xsl:value-of select="geo/long"/>
    <xsl:text>', '</xsl:text>
    <xsl:value-of select="@post"/>
    <xsl:text>', (SELECT code FROM country0 WHERE name = '</xsl:text>
    <xsl:value-of select="../@co_name"/>
    <xsl:text>'))</xsl:text>
    <xsl:if test="position() != last()">
      <xsl:text>;</xsl:text>
    </xsl:if>
  </xsl:template>

</xsl:stylesheet>
