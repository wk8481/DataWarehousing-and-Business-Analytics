<xsl:stylesheet version="1.0"  xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output method="text" omit-xml-declaration="yes"/>
	

    <!-- End of transaction-like block -->
<xsl:template match="/">
	<xsl:text>SET QUOTED_IDENTIFIER OFF;&#10;</xsl:text>
    <xsl:for-each select="CountryList/country/city">
        <xsl:text>INSERT INTO [catchem].[dbo].[city2] (city_id, city_name, latitude, longitude, postal_code, country_code) VALUES 
					(CAST('' AS XML).value('base64Binary("</xsl:text>
        <xsl:value-of select="@city_id"/>
        <xsl:text>")', 'VARBINARY(MAX)'),</xsl:text>
		<xsl:text>N'</xsl:text>
		<xsl:value-of select="@ci_name"/>
		<xsl:text>', </xsl:text>
		<xsl:value-of select="geo/lat"/>
		<xsl:text>, </xsl:text>	
		<xsl:value-of select="geo/long"/>	
		<xsl:text>, '</xsl:text>
        <xsl:value-of select="@post"/>
		<xsl:text>', '</xsl:text>
		<xsl:value-of select="/CountryList/country/@sc"/>
        <xsl:text>');&#10;</xsl:text>
	</xsl:for-each>
	
		<xsl:text>SET QUOTED_IDENTIFIER ON;&#10;</xsl:text>
   </xsl:template>

</xsl:stylesheet>
