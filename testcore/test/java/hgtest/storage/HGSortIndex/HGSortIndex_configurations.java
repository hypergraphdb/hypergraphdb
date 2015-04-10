package hgtest.storage.HGSortIndex;

import com.google.code.multitester.annonations.ImportedTest;

/**
 * @author Yuriy Sechko
 */
public class HGSortIndex_configurations
{
}

@ImportedTest(testClass = hgtest.storage.bje.DefaultIndexImpl.DefaultIndexImplTestBasis.class, startupSequence = {
		"up1", "up2" }, shutdownSequence = { "down1" })
class BJE_DefaultIndexImpl_configuration
{
}

@ImportedTest(testClass = hgtest.storage.bdb.DefaultIndexImpl.DefaultIndexImpl_countTest.class, startupSequence = {
		"up1", "up2" }, shutdownSequence = { "down1" })
class BDB_DefaultIndexImpl_configuration
{
}