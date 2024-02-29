<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">
    <xsl:output method="text" omit-xml-declaration="yes"/>
 
    <xsl:template match="/">
        <xsl:text>USE [catchem]&#xA;</xsl:text>
        <xsl:text>GO&#xA;&#xA;</xsl:text>
        <xsl:text>INSERT INTO [dbo].[country2]&#xA;</xsl:text>
        <xsl:text>           ([code]&#xA;</xsl:text>
        <xsl:text>           ,[code3]&#xA;</xsl:text>
        <xsl:text>           ,[name])&#xA;</xsl:text>
        <xsl:text>     VALUES&#xA;</xsl:text>
        <xsl:for-each select="//country">
            <xsl:text>           ('</xsl:text>
            <xsl:value-of select="@sc"/>
            <xsl:text>'</xsl:text>
            <xsl:text>           ,'</xsl:text>
            <xsl:value-of select="@lc"/>
            <xsl:text>'</xsl:text>
            <xsl:text>           ,'</xsl:text>
            <xsl:value-of select="@co_name"/>
            <xsl:text>')&#xA;</xsl:text>
            <xsl:if test="position() != last()">
                <xsl:text>     ,&#xA;</xsl:text>
            </xsl:if>
            <xsl:if test="position() = last()">
                <xsl:text>GO</xsl:text>
            </xsl:if>
            <xsl:text>&#xA;</xsl:text>
        </xsl:for-each>
    </xsl:template>
</xsl:stylesheet>
