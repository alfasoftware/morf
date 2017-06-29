/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.jdbc;

import java.sql.SQLException;


/**
 * Used to indicate that the entity has been modified in the meantime which
 * causes optimistic lock check to fail.
 * 
 * @author Copyright (c) Alfa Financial Software 2015
 */
public class OptimisticLockException extends SQLException {

  private static final long serialVersionUID = -7013600976501943538L;

}
