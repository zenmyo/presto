/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.facebook.presto.decoder.dummy.DummyRowDecoder;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;

public class KafkaTableDescriptionSupplier
        implements Supplier<Map<SchemaTableName, KafkaTopicDescription>>
{
    private static final Logger log = Logger.get(KafkaTableDescriptionSupplier.class);

    private final JsonCodec<KafkaTopicDescription> topicDescriptionCodec;
    private final File tableDescriptionDir;
    private final String defaultSchema;
    private final Properties properties;

    @Inject
    KafkaTableDescriptionSupplier(KafkaConnectorConfig kafkaConnectorConfig,
            JsonCodec<KafkaTopicDescription> topicDescriptionCodec)
    {
        this.topicDescriptionCodec = requireNonNull(topicDescriptionCodec, "topicDescriptionCodec is null");

        requireNonNull(kafkaConnectorConfig, "kafkaConfig is null");
        this.tableDescriptionDir = kafkaConnectorConfig.getTableDescriptionDir();
        this.defaultSchema = kafkaConnectorConfig.getDefaultSchema();
        this.properties = new Properties();
        this.properties.put("bootstrap.servers", kafkaConnectorConfig.getNodes().stream().map(h -> String.format("%s:%s", h.getHostText(), h.getPort())).collect(Collectors.joining(",")));
    }

    @Override
    public Map<SchemaTableName, KafkaTopicDescription> get()
    {
        ImmutableMap.Builder<SchemaTableName, KafkaTopicDescription> builder = ImmutableMap.builder();

        log.debug("Loading kafka table definitions from %s", tableDescriptionDir.getAbsolutePath());

        try (AdminClient adminClient = AdminClient.create(properties)) {
            Collection<TopicListing> topics = adminClient.listTopics().listings().get();
            for (TopicListing topic : topics) {
                File file = new File(tableDescriptionDir, topic.name() + ".json");
                if (file.isFile() && file.getName().endsWith(".json")) {
                    KafkaTopicDescription table = topicDescriptionCodec.fromJson(readAllBytes(file.toPath()));
                    String schemaName = firstNonNull(table.getSchemaName(), defaultSchema);
                    log.debug("Found Table definition for %s: %s", table.getTableName(), topic.name());
                    builder.put(new SchemaTableName(schemaName, table.getTableName()), table);
                }
                else {
                    SchemaTableName tableName = new SchemaTableName(defaultSchema, topic.name());
                    log.debug("Created dummy Table definition for %s", tableName);
                    builder.put(tableName,
                            new KafkaTopicDescription(tableName.getTableName(),
                            tableName.getSchemaName(),
                            topic.name(),
                            new KafkaTopicFieldGroup(DummyRowDecoder.NAME, ImmutableList.of()),
                            new KafkaTopicFieldGroup(DummyRowDecoder.NAME, ImmutableList.of())));
                }
            }
        }
        catch (IOException e) {
            log.warn(e, "Error: ");
            throw Throwables.propagate(e);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
        catch (ExecutionException e) {
            e.printStackTrace();
        }
        return builder.build();
    }

    /*
    private static List<File> listFiles(File dir)
    {
        if ((dir != null) && dir.isDirectory()) {
            File[] files = dir.listFiles();
            if (files != null) {
                log.debug("Considering files: %s", asList(files));
                return ImmutableList.copyOf(files);
            }
        }
        return ImmutableList.of();
    }
    */

    private static SchemaTableName parseTableName(String schemaTableName)
    {
        checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
        List<String> parts = Splitter.on('.').splitToList(schemaTableName);
        checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
        return new SchemaTableName(parts.get(0), parts.get(1));
    }
}
