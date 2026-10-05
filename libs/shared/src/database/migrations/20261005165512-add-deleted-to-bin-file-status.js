'use strict';

/** @type {import('sequelize-cli').Migration} */
module.exports = {
  async up(queryInterface) {
    await queryInterface.sequelize.query(
      `ALTER TYPE "enum_bin_files_status" ADD VALUE IF NOT EXISTS 'deleted';`,
    );
  },

  async down() {},
};
