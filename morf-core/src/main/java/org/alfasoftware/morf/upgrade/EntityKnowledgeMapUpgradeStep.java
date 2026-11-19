package org.alfasoftware.morf.upgrade;

import java.util.function.Predicate;
import java.util.stream.Stream;

public class EntityKnowledgeMapUpgradeStep {
  private final String name;
  private final String description;
  private final String jiraID;
  public EntityKnowledgeMapUpgradeStep(String currentUpgradeName, String description, String jiraID) {
    this.name = currentUpgradeName;
    this.description = description;
    this.jiraID = jiraID;
  }
  public String getName() {
    return name;
  }
  public String getDescription() {
    return description;
  }
  public String getJiraID() {
    return jiraID;
  }
  public boolean isPopulated(){
    return Stream.of(name, description,jiraID).anyMatch(Predicate.not(String::isBlank));
  }
}
