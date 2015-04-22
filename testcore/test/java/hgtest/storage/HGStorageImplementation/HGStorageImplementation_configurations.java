package hgtest.storage.HGStorageImplementation;

import com.google.code.multitester.annonations.ImportedTest;
import hgtest.storage.bdb.BDBStorageImplementation.BDBStorageImplementationTestBasis;
import hgtest.storage.bje.BJEStorageImplementation.BJEStorageImplementationTestBasis;

/**
 * Placeholder for test cases configurations.
 * 
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_configurations
{
}

@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
		"up1", "up_1" }, shutdownSequence = { "down2", "down1" })
class BJE_HGStorageImplementation_1
{
}

@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
		"up1", "up_1" }, shutdownSequence = { "down2", "down1" })
class BDB_HGStorageImplementation_1
{
}

@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
		"up1", "up_2" }, shutdownSequence = { "down2", "down1" })
class BJE_HGStorageImplementation_2
{
}

@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
		"up1", "up_2" }, shutdownSequence = { "down2", "down1" })
class BDB_HGStorageImplementation_2
{
}

@ImportedTest(testClass = BJEStorageImplementationTestBasis.class, startupSequence = {
		"up1", "up_4" }, shutdownSequence = { "down2", "down1" })
class BJE_HGStorageImplementation_4
{
}

@ImportedTest(testClass = BDBStorageImplementationTestBasis.class, startupSequence = {
		"up1", "up_4" }, shutdownSequence = { "down2", "down1" })
class BDB_HGStorageImplementation_4
{
}
