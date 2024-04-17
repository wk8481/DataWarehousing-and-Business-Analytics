<?xml version="1.0" encoding="UTF-8"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="text" encoding="UTF-8"/>

    <!-- template for each city -->
    <xsl:template match="city">
        <xsl:text>INSERT INTO [dbo].[city2] ([city_id], [city_name], [latitude], [longitude], [postal_code], [country_code]) VALUES (CONVERT(VARBINARY(MAX), CONVERT(XML, '').value('xs:base64Binary("</xsl:text>
        <xsl:value-of select="@city_id"/>
        <xsl:text>")', 'VARBINARY(MAX)'), 2), N'</xsl:text>
        <!-- call template to handle single quotes in name of cities -->
        <xsl:call-template name="handle-quotes">
            <xsl:with-param name="text" select="@ci_name"/>
        </xsl:call-template>
        <xsl:text>', </xsl:text>
        <xsl:value-of select="geo/lat"/>
        <xsl:text>, </xsl:text>
        <xsl:value-of select="geo/long"/>
        <xsl:text>, N'</xsl:text>
        <xsl:value-of select="@post"/>
        <xsl:text>', N'</xsl:text>
        <!-- Fetch the country code from the parent country element -->
        <xsl:value-of select="../@sc"/>
        <xsl:text>');</xsl:text>
        <xsl:if test="position() != last()">
            <xsl:text>
</xsl:text>
        </xsl:if>
    </xsl:template>

    <!-- template to handle single quotes by replacing them with two single quotes -->
    <xsl:template name="handle-quotes">
        <xsl:param name="text"/>
        <xsl:variable name="singleQuote" select='"&apos;"'/>
        <xsl:variable name="doubleSingleQuotes" select="concat($singleQuote, $singleQuote)" />
        <xsl:variable name="replacement" select="'&quot;&quot;'"/>




        <!-- Check if the text contains single quotes -->
        <xsl:choose>
            <!-- Handle two single quotes together -->
            <xsl:when test="contains($text, $doubleSingleQuotes)">
                <xsl:value-of select="translate($text, $doubleSingleQuotes, $replacement)" />
            </xsl:when>
            <!-- Handle single quote -->
            <xsl:when test="contains($text, $singleQuote)">
                <xsl:value-of select="translate($text, $singleQuote, $replacement)" />
            </xsl:when>
            <!-- If no single quotes, return the text as is -->
            <xsl:otherwise>
                <xsl:value-of select="$text"/>
            </xsl:otherwise>
        </xsl:choose>
    </xsl:template>


    <!-- process cities -->
    <xsl:template match="/">
        <xsl:apply-templates select="CountryList/country/city"/>
    </xsl:template>
</xsl:stylesheet>
